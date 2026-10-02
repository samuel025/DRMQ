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
	"sync/atomic"
	"time"

	pb "github.com/samuel025/DRMQ/drmq-go-client/protocol"
	"google.golang.org/protobuf/proto"
)

const (
	defaultBatchSizeBytes  = 16384
	defaultLingerMs        = 5
	defaultMaxInflight     = 5
	maxAccumulatorMessages = 10_000
	ackTimeoutMs           = 10_000
	inflightTimeoutMs      = 120_000
	maxPayloadSize         = 10 * 1024 * 1024
)


type SendResult struct {
	Success      bool
	Offset       int64
	ErrorMessage string
}

func (r SendResult) String() string {
	if r.Success {
		return fmt.Sprintf("SendResult{success=true, offset=%d}", r.Offset)
	}
	return fmt.Sprintf("SendResult{success=false, error='%s'}", r.ErrorMessage)
}


type SendFuture struct {
	ch chan sendOutcome
}

type sendOutcome struct {
	result SendResult
	err    error
}


func (f *SendFuture) Get() (SendResult, error) {
	out := <-f.ch
	return out.result, out.err
}


func (f *SendFuture) GetWithTimeout(timeout time.Duration) (SendResult, error) {
	select {
	case out := <-f.ch:
		return out.result, out.err
	case <-time.After(timeout):
		return SendResult{}, fmt.Errorf("send result timed out after %v", timeout)
	}
}

func newSendFuture() *SendFuture {
	return &SendFuture{ch: make(chan sendOutcome, 1)}
}

func (f *SendFuture) complete(result SendResult) {
	f.ch <- sendOutcome{result: result}
}

func (f *SendFuture) fail(err error) {
	f.ch <- sendOutcome{err: err}
}


type AtomicSendFuture struct {
	ch chan atomicSendOutcome
}

type atomicSendOutcome struct {
	offsets map[string]int64
	err     error
}


func (f *AtomicSendFuture) Get() (map[string]int64, error) {
	out := <-f.ch
	return out.offsets, out.err
}


func (f *AtomicSendFuture) GetWithTimeout(timeout time.Duration) (map[string]int64, error) {
	select {
	case out := <-f.ch:
		return out.offsets, out.err
	case <-time.After(timeout):
		return nil, fmt.Errorf("atomic send result timed out after %v", timeout)
	}
}

func newAtomicSendFuture() *AtomicSendFuture {
	return &AtomicSendFuture{ch: make(chan atomicSendOutcome, 1)}
}

func (f *AtomicSendFuture) complete(offsets map[string]int64) {
	f.ch <- atomicSendOutcome{offsets: offsets}
}

func (f *AtomicSendFuture) fail(err error) {
	f.ch <- atomicSendOutcome{err: err}
}


type ProducerConfig struct {

	BootstrapServers string


	BatchSizeBytes int


	LingerMs int64


	MaxInflight int


	Logger *slog.Logger
}


type Producer struct {
	host string
	port int

	bootstrapServers []serverAddr
	currentIndex     int

	batchSizeBytes int
	lingerMs       int64
	maxInflight    int

	conn      net.Conn
	connected bool

	writeMu   sync.Mutex
	readMu    sync.Mutex
	connectMu sync.Mutex

	running        atomic.Bool
	correlationSeq atomic.Int64

	inflightBatches sync.Map
	inflightSem     chan struct{}

	accumulator     chan *pendingMessage
	atomicAccum     chan *pendingAtomicMessage
	retryQueue      chan []*pendingMessage
	atomicRetryQ    chan []*pendingAtomicMessage

	wg sync.WaitGroup

	logger *slog.Logger
}

type pendingMessage struct {
	topic     string
	payload   []byte
	key       string
	timestamp int64
	future    *SendFuture
	entry     *pb.ProduceBatchRequest_BatchEntry
}

type pendingAtomicMessage struct {
	topicMessages map[string][]byte
	timestamp     int64
	future        *AtomicSendFuture
}

type inflightBatch struct {
	correlationID int64
	topic         string
	regularBatch  []*pendingMessage
	atomicData    *atomicInflightData
	sentAtMs      int64
}

type atomicInflightData struct {
	batch           []*pendingAtomicMessage
	relativeIndices map[int]map[string]int
}


func NewProducer(cfg ProducerConfig) (*Producer, error) {
	servers, err := parseBootstrapServers(cfg.BootstrapServers)
	if err != nil {
		return nil, err
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	batchSize := cfg.BatchSizeBytes
	if batchSize <= 0 {
		batchSize = defaultBatchSizeBytes
	}
	lingerMs := cfg.LingerMs
	if lingerMs <= 0 {
		lingerMs = defaultLingerMs
	}
	maxInflight := cfg.MaxInflight
	if maxInflight <= 0 {
		maxInflight = defaultMaxInflight
	}

	idx := rand.IntN(len(servers))

	p := &Producer{
		host:             servers[idx].host,
		port:             servers[idx].port,
		bootstrapServers: servers,
		currentIndex:     idx,
		batchSizeBytes:   batchSize,
		lingerMs:         lingerMs,
		maxInflight:      maxInflight,
		inflightSem:      make(chan struct{}, maxInflight),
		accumulator:      make(chan *pendingMessage, maxAccumulatorMessages),
		atomicAccum:      make(chan *pendingAtomicMessage, maxAccumulatorMessages),
		retryQueue:       make(chan []*pendingMessage, 1024),
		atomicRetryQ:     make(chan []*pendingAtomicMessage, 1024),
		logger:           logger,
	}
	p.running.Store(true)


	for i := 0; i < maxInflight; i++ {
		p.inflightSem <- struct{}{}
	}

	p.startBackgroundLoops()
	return p, nil
}


func (p *Producer) Connect() error {
	return p.ensureConnected()
}


func (p *Producer) Send(topic string, payload []byte) *SendFuture {
	return p.SendWithKey(topic, payload, "")
}


func (p *Producer) SendString(topic string, message string) *SendFuture {
	return p.Send(topic, []byte(message))
}


func (p *Producer) SendWithKey(topic string, payload []byte, key string) *SendFuture {
	future := newSendFuture()

	if len(payload) > maxPayloadSize {
		future.fail(fmt.Errorf("payload too large: %d bytes (max %d)", len(payload), maxPayloadSize))
		return future
	}

	msg := &pendingMessage{
		topic:     topic,
		payload:   payload,
		key:       key,
		timestamp: time.Now().UnixMilli(),
		future:    future,
	}


	entry := &pb.ProduceBatchRequest_BatchEntry{
		Payload:         payload,
		ClientTimestamp: msg.timestamp,
	}
	if key != "" {
		entry.Key = &key
	}
	msg.entry = entry

	select {
	case p.accumulator <- msg:
	default:
		future.fail(fmt.Errorf("producer accumulator is full (capacity: %d)", maxAccumulatorMessages))
	}
	return future
}


func (p *Producer) SendAtomic(topicMessages map[string][]byte) *AtomicSendFuture {
	future := newAtomicSendFuture()
	if len(topicMessages) < 2 {
		future.fail(fmt.Errorf("sendAtomic requires at least 2 topics, got %d", len(topicMessages)))
		return future
	}

	msg := &pendingAtomicMessage{
		topicMessages: topicMessages,
		timestamp:     time.Now().UnixMilli(),
		future:        future,
	}

	select {
	case p.atomicAccum <- msg:
	default:
		future.fail(fmt.Errorf("atomic accumulator is full"))
	}
	return future
}


func (p *Producer) IsConnected() bool {
	return p.connected && p.conn != nil
}


func (p *Producer) Close() {
	p.running.Store(false)
	p.connectMu.Lock()
	p.closeConnection()
	p.connectMu.Unlock()


	p.wg.Wait()


	p.failAllInflight(fmt.Errorf("producer closing"))
	p.logger.Info("disconnected from broker")
}



func (p *Producer) startBackgroundLoops() {
	p.wg.Add(4)

	go func() {
		defer p.wg.Done()
		p.senderLoop()
	}()

	go func() {
		defer p.wg.Done()
		p.atomicSenderLoop()
	}()

	go func() {
		defer p.wg.Done()
		p.readerLoop()
	}()

	go func() {
		defer p.wg.Done()
		p.reaperLoop()
	}()
}

func (p *Producer) senderLoop() {
	var leftover *pendingMessage

	for p.running.Load() || len(p.accumulator) > 0 || leftover != nil || len(p.retryQueue) > 0 {

		select {
		case retryBatch := <-p.retryQueue:
			if len(retryBatch) > 0 {
				p.sendBatchFireAndForget(retryBatch[0].topic, retryBatch)
				continue
			}
		default:
		}


		var firstMsg *pendingMessage
		if leftover != nil {
			firstMsg = leftover
			leftover = nil
		} else {
			select {
			case msg := <-p.accumulator:
				firstMsg = msg
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		if firstMsg == nil {
			continue
		}

		batch := []*pendingMessage{firstMsg}
		currentBytes := len(firstMsg.payload)
		currentTopic := firstMsg.topic


		if currentBytes < p.batchSizeBytes {
			leftover = p.drainAccumulator(&batch, &currentBytes, currentTopic)
		}


		if currentBytes < p.batchSizeBytes && leftover == nil {
			leftover = p.lingerForMore(&batch, &currentBytes, currentTopic)
		}


		if len(batch) > 0 {
			p.sendBatchFireAndForget(currentTopic, batch)
		}
	}
}

func (p *Producer) drainAccumulator(batch *[]*pendingMessage, currentBytes *int, topic string) *pendingMessage {
	for i := 0; i < 1024; i++ {
		select {
		case msg := <-p.accumulator:
			if msg.topic != topic {
				return msg
			}
			*batch = append(*batch, msg)
			*currentBytes += len(msg.payload)
			if *currentBytes >= p.batchSizeBytes {
				return nil
			}
		default:
			return nil
		}
	}
	return nil
}

func (p *Producer) lingerForMore(batch *[]*pendingMessage, currentBytes *int, topic string) *pendingMessage {
	deadline := time.Now().Add(time.Duration(p.lingerMs) * time.Millisecond)
	for *currentBytes < p.batchSizeBytes {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		select {
		case msg := <-p.accumulator:
			if msg.topic != topic {
				return msg
			}
			*batch = append(*batch, msg)
			*currentBytes += len(msg.payload)
		case <-time.After(remaining):
			return nil
		}
	}
	return nil
}

func (p *Producer) atomicSenderLoop() {
	for p.running.Load() || len(p.atomicAccum) > 0 || len(p.atomicRetryQ) > 0 {

		select {
		case retryBatch := <-p.atomicRetryQ:
			if len(retryBatch) > 0 {
				p.sendAtomicBatchFireAndForget(retryBatch)
				continue
			}
		default:
		}

		var firstMsg *pendingAtomicMessage
		select {
		case msg := <-p.atomicAccum:
			firstMsg = msg
		case <-time.After(100 * time.Millisecond):
			continue
		}
		if firstMsg == nil {
			continue
		}

		batch := []*pendingAtomicMessage{firstMsg}
		currentBytes := p.atomicMsgBytes(firstMsg)


		deadline := time.Now().Add(time.Duration(p.lingerMs) * time.Millisecond)
		for currentBytes < p.batchSizeBytes {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			select {
			case msg := <-p.atomicAccum:
				batch = append(batch, msg)
				currentBytes += p.atomicMsgBytes(msg)
			case <-time.After(remaining):
			}
		}

		if len(batch) > 0 {
			p.sendAtomicBatchFireAndForget(batch)
		}
	}
}

func (p *Producer) atomicMsgBytes(msg *pendingAtomicMessage) int {
	total := 0
	for _, payload := range msg.topicMessages {
		total += len(payload)
	}
	return total
}

func (p *Producer) readerLoop() {
	for p.running.Load() || p.hasInflightBatches() {
		if !p.connected || p.conn == nil {
			if p.hasInflightBatches() {
				p.failAllInflight(fmt.Errorf("connection lost"))
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}

		data, err := p.readResponseBytes()
		if err != nil || data == nil {
			continue
		}

		p.processResponseEnvelope(data)
	}
}

func (p *Producer) reaperLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for p.running.Load() {
		<-ticker.C
		if !p.connected {
			continue
		}

		hasStuck := false
		now := time.Now().UnixMilli()
		p.inflightBatches.Range(func(_, value any) bool {
			batch := value.(*inflightBatch)
			if now-batch.sentAtMs > ackTimeoutMs {
				hasStuck = true
				return false
			}
			return true
		})

		if hasStuck {
			p.logger.Warn("inflight batch timed out, force-closing connection", "timeoutMs", ackTimeoutMs)
			p.connectMu.Lock()
			p.closeConnection()
			p.rotateServer()
			p.connectMu.Unlock()
		}
	}
}



func (p *Producer) sendBatchFireAndForget(topic string, batch []*pendingMessage) {
	reqBuilder := &pb.ProduceBatchRequest{
		Topic: topic,
	}
	for _, pm := range batch {
		reqBuilder.Entries = append(reqBuilder.Entries, pm.entry)
	}

	if !p.acquireInflightPermit(batch) {
		return
	}

	corrID := p.correlationSeq.Add(1)

	payload, err := proto.Marshal(reqBuilder)
	if err != nil {
		p.releaseInflightPermit()
		for _, pm := range batch {
			pm.future.fail(fmt.Errorf("marshal batch request: %w", err))
		}
		return
	}

	envelope := &pb.MessageEnvelope{
		Type:          pb.MessageType_PRODUCE_BATCH_REQUEST,
		Payload:       payload,
		CorrelationId: corrID,
	}

	p.inflightBatches.Store(corrID, &inflightBatch{
		correlationID: corrID,
		topic:         topic,
		regularBatch:  batch,
		sentAtMs:      time.Now().UnixMilli(),
	})

	if err := p.writeEnvelope(envelope); err != nil {
		p.handleSendFailure(corrID, err,
			func() { p.requeueRetry(batch) },
			func() { p.failBatch(batch, fmt.Errorf("send batch failed: %w", err)) },
		)
	}
}

func (p *Producer) sendAtomicBatchFireAndForget(batch []*pendingAtomicMessage) {
	relativeIndices := make(map[int]map[string]int)
	req := p.buildAtomicRequest(batch, relativeIndices)

	if !p.acquireAtomicInflightPermit(batch) {
		return
	}

	corrID := p.correlationSeq.Add(1)

	payload, err := proto.Marshal(req)
	if err != nil {
		p.releaseInflightPermit()
		for _, pm := range batch {
			pm.future.fail(fmt.Errorf("marshal atomic request: %w", err))
		}
		return
	}

	envelope := &pb.MessageEnvelope{
		Type:          pb.MessageType_ATOMIC_PRODUCE_REQUEST,
		Payload:       payload,
		CorrelationId: corrID,
	}

	p.inflightBatches.Store(corrID, &inflightBatch{
		correlationID: corrID,
		atomicData: &atomicInflightData{
			batch:           batch,
			relativeIndices: relativeIndices,
		},
		sentAtMs: time.Now().UnixMilli(),
	})

	if err := p.writeEnvelope(envelope); err != nil {
		p.handleSendFailure(corrID, err,
			func() { p.requeueAtomicRetry(batch) },
			func() { p.failAtomicBatch(batch, fmt.Errorf("send atomic batch failed: %w", err)) },
		)
	}
}

func (p *Producer) buildAtomicRequest(batch []*pendingAtomicMessage, relativeIndicesOut map[int]map[string]int) *pb.AtomicProduceRequest {
	sliceBuilders := make(map[string]*pb.AtomicBatchTopicSlice)

	for i, pm := range batch {
		reqIndices := make(map[string]int)
		relativeIndicesOut[i] = reqIndices

		for topic, payload := range pm.topicMessages {
			slice, ok := sliceBuilders[topic]
			if !ok {
				slice = &pb.AtomicBatchTopicSlice{Topic: topic}
				sliceBuilders[topic] = slice
			}

			currentIndex := len(slice.Entries)
			reqIndices[topic] = currentIndex

			entry := &pb.ProduceBatchRequest_BatchEntry{
				Payload:         payload,
				ClientTimestamp: pm.timestamp,
			}
			slice.Entries = append(slice.Entries, entry)
		}
	}

	req := &pb.AtomicProduceRequest{}
	for _, slice := range sliceBuilders {
		req.Slices = append(req.Slices, slice)
	}
	return req
}



func (p *Producer) readResponseBytes() ([]byte, error) {
	conn := p.conn
	if conn == nil {
		return nil, nil
	}

	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		if !p.running.Load() && !p.hasInflightBatches() {
			return nil, nil
		}
		if p.running.Load() {
			p.logger.Debug("reader: connection lost", "error", err)
			p.connectMu.Lock()
			p.closeConnection()
			p.rotateServer()
			p.connectMu.Unlock()
			p.failAllInflight(err)
		}
		return nil, err
	}

	length := binary.BigEndian.Uint32(header)
	data := make([]byte, length)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (p *Producer) processResponseEnvelope(data []byte) {
	var respEnv pb.MessageEnvelope
	if err := proto.Unmarshal(data, &respEnv); err != nil {
		p.logger.Warn("failed to unmarshal response envelope", "error", err)
		return
	}

	corrID := respEnv.CorrelationId
	batchVal, ok := p.inflightBatches.LoadAndDelete(corrID)
	if !ok {
		p.logger.Warn("received response for unknown correlation ID", "correlationID", corrID)
		return
	}
	batch := batchVal.(*inflightBatch)
	p.releaseInflightPermit()

	switch respEnv.Type {
	case pb.MessageType_PRODUCE_BATCH_RESPONSE:
		p.processProduceBatchResponse(&respEnv, batch)
	case pb.MessageType_ATOMIC_PRODUCE_RESPONSE:
		p.processAtomicProduceResponse(&respEnv, batch)
	default:
		p.logger.Warn("unexpected response type", "type", respEnv.Type)
		p.failInflightBatch(batch, fmt.Errorf("unexpected response type: %v", respEnv.Type))
	}
}

func (p *Producer) processProduceBatchResponse(respEnv *pb.MessageEnvelope, batch *inflightBatch) {
	var resp pb.ProduceBatchResponse
	if err := proto.Unmarshal(respEnv.Payload, &resp); err != nil {
		p.failInflightBatch(batch, err)
		return
	}

	if resp.Success {
		baseOffset := resp.BaseOffset
		if batch.regularBatch != nil {
			for i, pm := range batch.regularBatch {
				pm.future.complete(SendResult{
					Success: true,
					Offset:  baseOffset + int64(i),
				})
			}
		}
	} else {
		errorCode := resp.ErrorCode
		errorMsg := resp.ErrorMessage

		if isNotLeaderError(errorCode, errorMsg) && p.running.Load() {
			p.handleNotLeaderRedirect(errorMsg)
			if batch.regularBatch != nil {
				p.requeueRetry(batch.regularBatch)
			}
		} else {
			if batch.regularBatch != nil {
				errStr := errorMsg
				if errorCode == pb.ErrorCode_NOT_LEADER {
					errStr = "NOT_LEADER"
				}
				for _, pm := range batch.regularBatch {
					pm.future.fail(fmt.Errorf("%s", errStr))
				}
			}
		}
	}
}

func (p *Producer) processAtomicProduceResponse(respEnv *pb.MessageEnvelope, batch *inflightBatch) {
	var resp pb.AtomicProduceResponse
	if err := proto.Unmarshal(respEnv.Payload, &resp); err != nil {
		p.failInflightBatch(batch, err)
		return
	}

	if batch.atomicData == nil {
		return
	}

	if resp.Success {
		baseOffsets := resp.BaseOffsets
		for i, pm := range batch.atomicData.batch {
			reqIndices := batch.atomicData.relativeIndices[i]
			finalOffsets := make(map[string]int64)

			for topic := range pm.topicMessages {
				base, ok := baseOffsets[topic]
				if ok && base != -1 {
					finalOffsets[topic] = base + int64(reqIndices[topic])
				}
			}
			pm.future.complete(finalOffsets)
		}
	} else {
		errorCode := resp.ErrorCode
		errorMsg := resp.ErrorMessage

		if isNotLeaderError(errorCode, errorMsg) && p.running.Load() {
			p.handleNotLeaderRedirect(errorMsg)
			p.requeueAtomicRetry(batch.atomicData.batch)
		} else {
			for _, pm := range batch.atomicData.batch {
				pm.future.fail(fmt.Errorf("atomic batch failed: %s", errorMsg))
			}
		}
	}
}



func (p *Producer) ensureConnected() error {
	p.connectMu.Lock()
	defer p.connectMu.Unlock()

	if p.connected && p.conn != nil {
		return nil
	}

	var lastErr error
	totalAttempts := maxRetries * max(1, len(p.bootstrapServers))

	for attempt := 0; attempt < totalAttempts; attempt++ {
		if err := p.connectInternal(); err != nil {
			p.logger.Debug("connection failed",
				"host", p.host, "port", p.port,
				"attempt", attempt+1, "total", totalAttempts, "error", err)
			lastErr = err
			p.closeConnection()
			p.rotateServer()
			time.Sleep(reconnectDelay)
			continue
		}
		return nil
	}

	return fmt.Errorf("failed to connect after %d attempts: %w", totalAttempts, lastErr)
}

func (p *Producer) connectInternal() error {
	if p.connected && p.conn != nil {
		return nil
	}
	p.connected = false
	p.closeConnection()

	addr := net.JoinHostPort(p.host, strconv.Itoa(p.port))
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}

	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}

	p.conn = conn
	p.connected = true
	p.logger.Info("connected to broker", "host", p.host, "port", p.port)
	return nil
}

func (p *Producer) writeEnvelope(envelope *pb.MessageEnvelope) error {
	if err := p.ensureConnected(); err != nil {
		return err
	}

	data, err := proto.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	p.writeMu.Lock()
	defer p.writeMu.Unlock()

	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(data)))

	if _, err := p.conn.Write(header); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if _, err := p.conn.Write(data); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}
	return nil
}

func (p *Producer) rotateServer() {
	if len(p.bootstrapServers) <= 1 {
		return
	}
	p.currentIndex = (p.currentIndex + 1) % len(p.bootstrapServers)
	srv := p.bootstrapServers[p.currentIndex]
	p.host = srv.host
	p.port = srv.port
	p.logger.Info("switching to next broker", "host", p.host, "port", p.port)
}

func (p *Producer) syncServerIndex() {
	for i, srv := range p.bootstrapServers {
		if srv.host == p.host && srv.port == p.port {
			p.currentIndex = i
			return
		}
	}
}

func (p *Producer) closeConnection() {
	p.connected = false
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
}

func (p *Producer) redirectToLeader(leaderAddr string) error {
	parts := strings.SplitN(leaderAddr, ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid leader address: %s", leaderAddr)
	}
	port, err := strconv.Atoi(parts[1])
	if err != nil {
		return fmt.Errorf("invalid port in leader address: %s", leaderAddr)
	}
	p.host = parts[0]
	p.port = port
	p.syncServerIndex()
	p.closeConnection()
	p.logger.Info("redirected to leader", "host", p.host, "port", p.port)
	return nil
}

func (p *Producer) handleNotLeaderRedirect(errorMsg string) {
	leaderAddr := extractLeaderAddress(errorMsg)
	if leaderAddr != "" {
		if err := p.redirectToLeader(leaderAddr); err != nil {
			p.logger.Warn("failed to redirect to leader", "leader", leaderAddr, "error", err)
		} else {
			return
		}
	}
	p.rotateServer()
	p.closeConnection()
}

func extractLeaderAddress(errorMsg string) string {
	if errorMsg == "" {
		return ""
	}
	const prefix = "NOT_LEADER:"
	idx := strings.Index(errorMsg, prefix)
	if idx == -1 {
		return ""
	}
	addr := strings.TrimSpace(errorMsg[idx+len(prefix):])
	if addr == "" || addr == "UNKNOWN" {
		return ""
	}
	parts := strings.SplitN(addr, ":", 2)
	if len(parts) != 2 {
		return ""
	}
	if _, err := strconv.Atoi(parts[1]); err != nil {
		return ""
	}
	return addr
}

func isNotLeaderError(errorCode pb.ErrorCode, errorMsg string) bool {
	return errorCode == pb.ErrorCode_NOT_LEADER ||
		(errorMsg != "" && strings.Contains(errorMsg, "NOT_LEADER"))
}



func (p *Producer) acquireInflightPermit(batch []*pendingMessage) bool {
	timer := time.NewTimer(time.Duration(inflightTimeoutMs) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-p.inflightSem:
		return true
	case <-timer.C:
		for _, pm := range batch {
			pm.future.fail(fmt.Errorf("inflight timeout: too many unacknowledged batches"))
		}
		return false
	}
}

func (p *Producer) acquireAtomicInflightPermit(batch []*pendingAtomicMessage) bool {
	timer := time.NewTimer(time.Duration(inflightTimeoutMs) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-p.inflightSem:
		return true
	case <-timer.C:
		for _, pm := range batch {
			pm.future.fail(fmt.Errorf("inflight timeout: too many unacknowledged batches"))
		}
		return false
	}
}

func (p *Producer) releaseInflightPermit() {
	select {
	case p.inflightSem <- struct{}{}:
	default:
	}
}

func (p *Producer) hasInflightBatches() bool {
	has := false
	p.inflightBatches.Range(func(_, _ any) bool {
		has = true
		return false
	})
	return has
}

func (p *Producer) handleSendFailure(corrID int64, err error, retryAction func(), failAction func()) {
	if _, ok := p.inflightBatches.LoadAndDelete(corrID); ok {
		p.releaseInflightPermit()
		p.closeConnection()
		if p.running.Load() {
			retryAction()
		} else {
			failAction()
		}
	}
}

func (p *Producer) failInflightBatch(batch *inflightBatch, cause error) {
	if batch.regularBatch != nil {
		for _, pm := range batch.regularBatch {
			pm.future.fail(cause)
		}
	}
	if batch.atomicData != nil {
		for _, pm := range batch.atomicData.batch {
			pm.future.fail(cause)
		}
	}
}

func (p *Producer) failAllInflight(cause error) {
	p.inflightBatches.Range(func(key, value any) bool {
		p.inflightBatches.Delete(key)
		batch := value.(*inflightBatch)
		p.releaseInflightPermit()
		if p.running.Load() {
			if batch.regularBatch != nil {
				p.requeueRetry(batch.regularBatch)
			} else if batch.atomicData != nil {
				p.requeueAtomicRetry(batch.atomicData.batch)
			}
		} else {
			p.failInflightBatch(batch, cause)
		}
		return true
	})
}

func (p *Producer) failBatch(batch []*pendingMessage, err error) {
	for _, pm := range batch {
		pm.future.fail(err)
	}
}

func (p *Producer) failAtomicBatch(batch []*pendingAtomicMessage, err error) {
	for _, pm := range batch {
		pm.future.fail(err)
	}
}

func (p *Producer) requeueRetry(batch []*pendingMessage) {
	select {
	case p.retryQueue <- batch:
	default:
		p.failBatch(batch, fmt.Errorf("retry queue full"))
	}
}

func (p *Producer) requeueAtomicRetry(batch []*pendingAtomicMessage) {
	select {
	case p.atomicRetryQ <- batch:
	default:
		p.failAtomicBatch(batch, fmt.Errorf("atomic retry queue full"))
	}
}
