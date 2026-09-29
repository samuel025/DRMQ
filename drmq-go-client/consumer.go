package drmq

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/drmq/drmq-go-client/protocol"
	"google.golang.org/protobuf/proto"
)

const (
	defaultPort           = 9092
	defaultMaxMessages    = 100
	defaultPollTimeoutMs  = 1000
	maxRetries            = 5
	reconnectDelay        = 500 * time.Millisecond
	socketBufferSize      = 65536
)

type ConsumedMessage struct {
	Offset    int64
	Topic     string
	Payload   []byte
	Key       string 
	Timestamp int64
	StoredAt  int64
}

func (m *ConsumedMessage) PayloadAsString() string {
	return string(m.Payload)
}


type ConsumerConfig struct {
	BootstrapServers string

	ConsumerGroup string

	AutoCommit bool

	Logger *slog.Logger
}

type Consumer struct {
	mu sync.Mutex

	host string
	port int

	consumerGroup string
	consumerID    string
	groupMode     bool
	autoCommit    bool

	bootstrapServers []serverAddr
	currentIndex     int

	conn      net.Conn
	connected bool

	topicOffsets map[string]int64

	logger *slog.Logger
}

type serverAddr struct {
	host string
	port int
}

func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	servers, err := parseBootstrapServers(cfg.BootstrapServers)
	if err != nil {
		return nil, err
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	idx := rand.IntN(len(servers))
	groupMode := cfg.ConsumerGroup != ""

	return &Consumer{
		host:             servers[idx].host,
		port:             servers[idx].port,
		consumerGroup:    cfg.ConsumerGroup,
		consumerID:       generateID(),
		groupMode:        groupMode,
		autoCommit:       cfg.AutoCommit,
		bootstrapServers: servers,
		currentIndex:     idx,
		topicOffsets:     make(map[string]int64),
		logger:           logger,
	}, nil
}

func (c *Consumer) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ensureConnected()
}

func (c *Consumer) Subscribe(topic string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureConnected(); err != nil {
		return err
	}

	if c.groupMode {
		c.topicOffsets[topic] = -1
		c.logger.Info("subscribed (group mode)",
			"topic", topic, "group", c.consumerGroup, "consumerID", c.consumerID)
	} else {
		c.topicOffsets[topic] = 0
		c.logger.Info("subscribed (single mode)", "topic", topic)
	}
	return nil
}

func (c *Consumer) SubscribeFrom(topic string, fromOffset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureConnected(); err != nil {
		return err
	}

	c.topicOffsets[topic] = fromOffset
	c.logger.Info("subscribed from explicit offset",
		"topic", topic, "offset", fromOffset, "group", c.consumerGroup)
	return nil
}

func (c *Consumer) SeekByTime(topic string, timestamp int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	offset, err := c.doSearchOffsetByTime(topic, timestamp)
	if err != nil {
		return err
	}
	if offset < 0 {
		c.logger.Warn("no offset found for timestamp", "topic", topic, "timestamp", timestamp)
		return nil
	}

	c.topicOffsets[topic] = offset
	if c.groupMode {
		if err := c.commitOffsetInternal(topic, offset); err != nil {
			return fmt.Errorf("commit after seekByTime: %w", err)
		}
	}
	c.logger.Info("subscribed from time-based offset",
		"topic", topic, "offset", offset, "timestamp", timestamp)
	return nil
}

func (c *Consumer) Poll() ([]ConsumedMessage, error) {
	return c.PollWithOptions(defaultMaxMessages, defaultPollTimeoutMs)
}

func (c *Consumer) PollMax(maxMessages int) ([]ConsumedMessage, error) {
	return c.PollWithOptions(maxMessages, defaultPollTimeoutMs)
}

func (c *Consumer) PollWithOptions(maxMessages int, timeoutMs int64) ([]ConsumedMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.ensureConnected(); err != nil {
		return nil, err
	}

	messages, err := c.doPoll(maxMessages, timeoutMs)
	if err != nil {
		c.logger.Warn("poll failed, reconnecting", "error", err)
		if reconnErr := c.reconnect(); reconnErr != nil {
			return nil, fmt.Errorf("poll failed and reconnect failed: %w (original: %v)", reconnErr, err)
		}
		messages, err = c.doPoll(maxMessages, timeoutMs)
		if err != nil {
			return nil, err
		}
	}
	return messages, nil
}

func (c *Consumer) Commit(topic string, offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.groupMode {
		return fmt.Errorf("commit is only supported in consumer group mode")
	}

	if err := c.ensureConnected(); err != nil {
		return err
	}

	c.topicOffsets[topic] = offset
	if err := c.commitOffsetInternal(topic, offset); err != nil {
		return err
	}
	c.logger.Debug("manually committed offset", "topic", topic, "offset", offset)
	return nil
}

func (c *Consumer) Nack(topic string, offset int64) (routedToDLQ bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.groupMode {
		return false, fmt.Errorf("nack is only supported in consumer group mode")
	}

	if err := c.ensureConnected(); err != nil {
		return false, err
	}

	return c.nackWithRetry(topic, offset)
}

func (c *Consumer) CurrentOffset(topic string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if off, ok := c.topicOffsets[topic]; ok {
		return off
	}
	return 0
}

func (c *Consumer) ConsumerID() string {
	return c.consumerID
}

func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected {
		return nil
	}
	c.closeConnection()
	c.logger.Info("disconnected from broker")
	return nil
}


func (c *Consumer) ensureConnected() error {
	if c.connected && c.conn != nil {
		return nil
	}

	var lastErr error
	totalAttempts := maxRetries * len(c.bootstrapServers)

	for attempt := 0; attempt < totalAttempts; attempt++ {
		if err := c.connectInternal(); err != nil {
			c.logger.Debug("connection failed",
				"host", c.host, "port", c.port,
				"attempt", attempt+1, "total", totalAttempts, "error", err)
			lastErr = err
			c.closeConnection()
			c.rotateServer()
			time.Sleep(reconnectDelay)
			continue
		}
		return nil
	}

	return fmt.Errorf("failed to connect after %d attempts: %w", totalAttempts, lastErr)
}

func (c *Consumer) connectInternal() error {
	if c.connected && c.conn != nil {
		return nil
	}
	c.connected = false
	c.closeConnection()

	addr := net.JoinHostPort(c.host, strconv.Itoa(c.port))
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}

	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}

	c.conn = conn
	c.connected = true
	c.logger.Info("connected to broker", "host", c.host, "port", c.port, "group", c.consumerGroup)
	return nil
}

func (c *Consumer) reconnect() error {
	c.closeConnection()
	c.rotateServer()
	if err := c.ensureConnected(); err != nil {
		return err
	}
	for topic, offset := range c.topicOffsets {
		c.logger.Info("re-subscribing after reconnect", "topic", topic, "offset", offset)
	}
	return nil
}

func (c *Consumer) rotateServer() {
	if len(c.bootstrapServers) <= 1 {
		return
	}
	c.currentIndex = (c.currentIndex + 1) % len(c.bootstrapServers)
	srv := c.bootstrapServers[c.currentIndex]
	c.host = srv.host
	c.port = srv.port
	c.logger.Info("switching to next broker", "host", c.host, "port", c.port)
}

func (c *Consumer) syncServerIndex() {
	for i, srv := range c.bootstrapServers {
		if srv.host == c.host && srv.port == c.port {
			c.currentIndex = i
			return
		}
	}
}

func (c *Consumer) closeConnection() {
	c.connected = false
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func (c *Consumer) tryRedirectToLeader(errorMsg string) (bool, error) {
	if errorMsg == "" || !strings.HasPrefix(errorMsg, "NOT_LEADER:") {
		return false, nil
	}

	leaderAddr := parseLeaderAddress(errorMsg)
	if leaderAddr != "" {
		parts := strings.SplitN(leaderAddr, ":", 2)
		port, _ := strconv.Atoi(parts[1])
		c.host = parts[0]
		c.port = port
		c.syncServerIndex()
		c.closeConnection()
		if err := c.ensureConnected(); err != nil {
			return false, err
		}
		c.logger.Info("redirected to leader", "host", c.host, "port", c.port)
		return true, nil
	}

	return true, c.reconnect()
}


func (c *Consumer) sendEnvelope(envelope *pb.MessageEnvelope) error {
	data, err := proto.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))

	if _, err := c.conn.Write(header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if _, err := c.conn.Write(data); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	return nil
}

func (c *Consumer) receiveEnvelope() (*pb.MessageEnvelope, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, header); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	length := binary.BigEndian.Uint32(header)
	data := make([]byte, length)
	if _, err := io.ReadFull(c.conn, data); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	var envelope pb.MessageEnvelope
	if err := proto.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("unmarshal envelope: %w", err)
	}
	return &envelope, nil
}


func (c *Consumer) doPoll(maxMessages int, timeoutMs int64) ([]ConsumedMessage, error) {
	var all []ConsumedMessage

	for topic, fromOffset := range c.topicOffsets {
		var offset int64
		if c.groupMode {
			offset = 0
		} else {
			offset = fromOffset
		}

		messages, err := c.fetchMessages(topic, offset, maxMessages, timeoutMs)
		if err != nil {
			return nil, err
		}
		all = append(all, messages...)

		if len(messages) > 0 {
			nextOffset := messages[len(messages)-1].Offset + 1
			c.topicOffsets[topic] = nextOffset
			if c.groupMode && c.autoCommit {
				if err := c.commitOffsetInternal(topic, nextOffset); err != nil {
					return nil, fmt.Errorf("auto-commit: %w", err)
				}
			}
		}
	}
	return all, nil
}

func (c *Consumer) fetchMessages(topic string, fromOffset int64, maxMessages int, timeoutMs int64) ([]ConsumedMessage, error) {
	return c.executeWithRetry("fetch messages", func() ([]ConsumedMessage, error) {
		return c.fetchMessagesInternal(topic, fromOffset, int32(maxMessages), timeoutMs)
	})
}

func (c *Consumer) fetchMessagesInternal(topic string, fromOffset int64, maxMessages int32, timeoutMs int64) ([]ConsumedMessage, error) {
	req := &pb.ConsumeRequest{
		Topic:       topic,
		FromOffset:  fromOffset,
		MaxMessages: maxMessages,
		TimeoutMs:   timeoutMs,
	}
	if c.groupMode {
		req.ConsumerGroup = &c.consumerGroup
		req.ConsumerId = &c.consumerID
	}

	payload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal consume request: %w", err)
	}

	envelope := &pb.MessageEnvelope{
		Type:    pb.MessageType_CONSUME_REQUEST,
		Payload: payload,
	}
	if err := c.sendEnvelope(envelope); err != nil {
		return nil, err
	}

	respEnv, err := c.receiveEnvelope()
	if err != nil {
		return nil, err
	}

	var resp pb.ConsumeResponse
	if err := proto.Unmarshal(respEnv.Payload, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal consume response: %w", err)
	}

	if !resp.Success {
		redirected, redirectErr := c.tryRedirectToLeader(resp.ErrorMessage)
		if redirectErr != nil {
			return nil, redirectErr
		}
		if redirected {
			return c.fetchMessagesInternal(topic, fromOffset, maxMessages, timeoutMs)
		}
		return nil, fmt.Errorf("consume failed: %s", resp.ErrorMessage)
	}

	messages := make([]ConsumedMessage, 0, len(resp.Messages))
	for _, msg := range resp.Messages {
		key := ""
		if msg.Key != nil {
			key = *msg.Key
		}
		messages = append(messages, ConsumedMessage{
			Offset:    msg.Offset,
			Topic:     msg.Topic,
			Payload:   msg.Payload,
			Key:       key,
			Timestamp: msg.Timestamp,
			StoredAt:  msg.StoredAt,
		})
	}

	c.logger.Debug("fetched messages", "topic", topic, "count", len(messages), "fromOffset", fromOffset)
	return messages, nil
}


func (c *Consumer) commitOffsetInternal(topic string, offset int64) error {
	_, err := c.executeWithRetry("commit offset", func() (any, error) {
		return nil, c.commitOffsetDirect(topic, offset)
	})
	return err
}

func (c *Consumer) commitOffsetDirect(topic string, offset int64) error {
	req := &pb.CommitOffsetRequest{
		Topic:  topic,
		Offset: offset,
	}
	if c.consumerGroup != "" {
		req.ConsumerGroup = c.consumerGroup
	}
	if c.groupMode {
		req.ConsumerId = &c.consumerID
	}

	payload, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal commit request: %w", err)
	}

	envelope := &pb.MessageEnvelope{
		Type:    pb.MessageType_COMMIT_OFFSET_REQUEST,
		Payload: payload,
	}
	if err := c.sendEnvelope(envelope); err != nil {
		return err
	}

	respEnv, err := c.receiveEnvelope()
	if err != nil {
		return err
	}

	var resp pb.CommitOffsetResponse
	if err := proto.Unmarshal(respEnv.Payload, &resp); err != nil {
		return fmt.Errorf("unmarshal commit response: %w", err)
	}

	if !resp.Success {
		redirected, redirectErr := c.tryRedirectToLeader(resp.ErrorMessage)
		if redirectErr != nil {
			return redirectErr
		}
		if redirected {
			return c.commitOffsetDirect(topic, offset)
		}
		c.logger.Warn("commit offset failed", "topic", topic, "error", resp.ErrorMessage)
	} else {
		c.logger.Debug("committed offset", "topic", topic, "offset", offset)
	}
	return nil
}

func (c *Consumer) nackWithRetry(topic string, offset int64) (bool, error) {
	return c.executeWithRetry("nack offset", func() (bool, error) {
		return c.nackInternal(topic, offset)
	})
}

func (c *Consumer) nackInternal(topic string, offset int64) (bool, error) {
	req := &pb.NackRequest{
		Topic:  topic,
		Offset: offset,
	}
	if c.consumerGroup != "" {
		req.ConsumerGroup = c.consumerGroup
	}
	if c.groupMode {
		req.ConsumerId = &c.consumerID
	}

	payload, err := proto.Marshal(req)
	if err != nil {
		return false, fmt.Errorf("marshal nack request: %w", err)
	}

	envelope := &pb.MessageEnvelope{
		Type:    pb.MessageType_NACK_REQUEST,
		Payload: payload,
	}
	if err := c.sendEnvelope(envelope); err != nil {
		return false, err
	}

	respEnv, err := c.receiveEnvelope()
	if err != nil {
		return false, err
	}

	var resp pb.NackResponse
	if err := proto.Unmarshal(respEnv.Payload, &resp); err != nil {
		return false, fmt.Errorf("unmarshal nack response: %w", err)
	}

	if !resp.Success {
		redirected, redirectErr := c.tryRedirectToLeader(resp.ErrorMessage)
		if redirectErr != nil {
			return false, redirectErr
		}
		if redirected {
			return c.nackInternal(topic, offset)
		}
		return false, fmt.Errorf("nack failed: %s", resp.ErrorMessage)
	}

	return resp.RoutedToDlq, nil
}


func (c *Consumer) doSearchOffsetByTime(topic string, timestamp int64) (int64, error) {
	return c.executeWithRetry("search offset by time", func() (int64, error) {
		return c.sendSearchOffsetRequest(topic, timestamp)
	})
}

func (c *Consumer) sendSearchOffsetRequest(topic string, timestamp int64) (int64, error) {
	req := &pb.SearchOffsetByTimeRequest{
		Topic:     topic,
		Timestamp: timestamp,
	}

	payload, err := proto.Marshal(req)
	if err != nil {
		return -1, fmt.Errorf("marshal search offset request: %w", err)
	}

	envelope := &pb.MessageEnvelope{
		Type:    pb.MessageType_SEARCH_OFFSET_BY_TIME_REQUEST,
		Payload: payload,
	}
	if err := c.sendEnvelope(envelope); err != nil {
		return -1, err
	}

	respEnv, err := c.receiveEnvelope()
	if err != nil {
		return -1, err
	}

	switch respEnv.Type {
	case pb.MessageType_SEARCH_OFFSET_BY_TIME_RESPONSE:
		var resp pb.SearchOffsetByTimeResponse
		if err := proto.Unmarshal(respEnv.Payload, &resp); err != nil {
			return -1, fmt.Errorf("unmarshal search offset response: %w", err)
		}
		return resp.Offset, nil

	case pb.MessageType_PRODUCE_RESPONSE:
		var errorResp pb.ProduceResponse
		if err := proto.Unmarshal(respEnv.Payload, &errorResp); err != nil {
			return -1, fmt.Errorf("unmarshal error response: %w", err)
		}
		redirected, redirectErr := c.tryRedirectToLeader(errorResp.ErrorMessage)
		if redirectErr != nil {
			return -1, redirectErr
		}
		if redirected {
			return c.sendSearchOffsetRequest(topic, timestamp)
		}
		return -1, fmt.Errorf("error searching offset by time: %s", errorResp.ErrorMessage)

	default:
		return -1, fmt.Errorf("unexpected response type: %v", respEnv.Type)
	}
}


func (c *Consumer) executeWithRetry[T any](opName string, op func() (T, error)) (T, error) {
	var lastErr error
	var zero T

	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := c.ensureConnected(); err != nil {
			lastErr = err
			c.closeConnection()
			c.rotateServer()
			time.Sleep(reconnectDelay)
			continue
		}

		result, err := op()
		if err == nil {
			return result, nil
		}

		lastErr = err
		c.logger.Warn("operation failed",
			"op", opName, "host", c.host, "port", c.port,
			"attempt", attempt+1, "maxRetries", maxRetries, "error", err)
		c.closeConnection()
		c.rotateServer()
		time.Sleep(reconnectDelay)
	}

	return zero, fmt.Errorf("failed to %s after %d attempts: %w", opName, maxRetries, lastErr)
}


func parseBootstrapServers(s string) ([]serverAddr, error) {
	if s == "" {
		return nil, fmt.Errorf("bootstrap servers string is empty")
	}

	parts := strings.Split(s, ",")
	var servers []serverAddr
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		hp := strings.SplitN(part, ":", 2)
		if len(hp) != 2 {
			continue
		}
		port, err := strconv.Atoi(hp[1])
		if err != nil {
			continue
		}
		servers = append(servers, serverAddr{host: hp[0], port: port})
	}

	if len(servers) == 0 {
		return nil, fmt.Errorf("no valid bootstrap servers in: %s", s)
	}
	return servers, nil
}

func parseLeaderAddress(errorMsg string) string {
	leader := strings.TrimPrefix(errorMsg, "NOT_LEADER:")
	if leader == "UNKNOWN" || leader == "" {
		return ""
	}
	parts := strings.SplitN(leader, ":", 2)
	if len(parts) != 2 {
		return ""
	}
	if _, err := strconv.Atoi(parts[1]); err != nil {
		return ""
	}
	return leader
}

func generateID() string {
	b := make([]byte, 16)
	for i := range b {
		b[i] = byte(rand.IntN(256))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.BigEndian.Uint32(b[0:4]),
		binary.BigEndian.Uint16(b[4:6]),
		binary.BigEndian.Uint16(b[6:8]),
		binary.BigEndian.Uint16(b[8:10]),
		b[10:16])
}
