package drmq

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	pb "github.com/drmq/drmq-go-client/protocol"
	"google.golang.org/protobuf/proto"
)

var testLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

func TestConsumer_ConnectAndClose(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	consumer, err := NewConsumer(ConsumerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	if err := consumer.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if err := consumer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestConsumer_ConnectFailure(t *testing.T) {
	_, err := NewConsumer(ConsumerConfig{
		BootstrapServers: "127.0.0.1:1",
		Logger:           testLogger,
	})
	if err != nil {
		t.Fatalf("NewConsumer should not fail: %v", err)
	}
}

func TestConsumer_SubscribeSingleMode(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	consumer, err := NewConsumer(ConsumerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()

	if err := consumer.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if err := consumer.Subscribe("test-topic"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if consumer.CurrentOffset("test-topic") != 0 {
		t.Errorf("expected offset 0 in single mode, got %d", consumer.CurrentOffset("test-topic"))
	}
}

func TestConsumer_SubscribeGroupMode(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	consumer, err := NewConsumer(ConsumerConfig{
		BootstrapServers: mb.addr,
		ConsumerGroup:    "my-group",
		Logger:           testLogger,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()

	if err := consumer.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if err := consumer.Subscribe("test-topic"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if consumer.CurrentOffset("test-topic") != -1 {
		t.Errorf("expected offset -1 in group mode, got %d", consumer.CurrentOffset("test-topic"))
	}

	if consumer.ConsumerID() == "" {
		t.Error("consumer ID should not be empty")
	}
}

func TestConsumer_SubscribeFrom(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	consumer, err := NewConsumer(ConsumerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()
	consumer.Connect()

	if err := consumer.SubscribeFrom("test-topic", 42); err != nil {
		t.Fatalf("SubscribeFrom: %v", err)
	}

	if consumer.CurrentOffset("test-topic") != 42 {
		t.Errorf("expected offset 42, got %d", consumer.CurrentOffset("test-topic"))
	}
}

func TestConsumer_PollSingleMode(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	mb.onMessage(pb.MessageType_CONSUME_REQUEST, func(env *pb.MessageEnvelope) *pb.MessageEnvelope {
		var req pb.ConsumeRequest
		proto.Unmarshal(env.Payload, &req)

		key := "key-1"
		resp := &pb.ConsumeResponse{
			Success: true,
			Messages: []*pb.StoredMessage{
				{
					Offset:    0,
					Topic:     req.Topic,
					Payload:   []byte("message-0"),
					Key:       &key,
					Timestamp: 1000,
					StoredAt:  1001,
				},
				{
					Offset:    1,
					Topic:     req.Topic,
					Payload:   []byte("message-1"),
					Timestamp: 1002,
					StoredAt:  1003,
				},
			},
		}

		payload := mustMarshal(t, resp)
		return &pb.MessageEnvelope{
			Type:    pb.MessageType_CONSUME_RESPONSE,
			Payload: payload,
		}
	})

	consumer, _ := NewConsumer(ConsumerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})
	defer consumer.Close()
	consumer.Connect()
	consumer.Subscribe("test-topic")

	messages, err := consumer.Poll()
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}

	if messages[0].PayloadAsString() != "message-0" {
		t.Errorf("msg[0] payload: got %q", messages[0].PayloadAsString())
	}
	if messages[0].Key != "key-1" {
		t.Errorf("msg[0] key: got %q, want %q", messages[0].Key, "key-1")
	}
	if messages[0].Offset != 0 {
		t.Errorf("msg[0] offset: got %d, want 0", messages[0].Offset)
	}
	if messages[1].PayloadAsString() != "message-1" {
		t.Errorf("msg[1] payload: got %q", messages[1].PayloadAsString())
	}
	if messages[1].Key != "" {
		t.Errorf("msg[1] key: expected empty, got %q", messages[1].Key)
	}

	if consumer.CurrentOffset("test-topic") != 2 {
		t.Errorf("expected offset 2 after poll, got %d", consumer.CurrentOffset("test-topic"))
	}
}

func TestConsumer_PollEmptyResponse(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	mb.onMessage(pb.MessageType_CONSUME_REQUEST, func(env *pb.MessageEnvelope) *pb.MessageEnvelope {
		resp := &pb.ConsumeResponse{Success: true, Messages: nil}
		return &pb.MessageEnvelope{
			Type:    pb.MessageType_CONSUME_RESPONSE,
			Payload: mustMarshal(t, resp),
		}
	})

	consumer, _ := NewConsumer(ConsumerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})
	defer consumer.Close()
	consumer.Connect()
	consumer.Subscribe("test-topic")

	messages, err := consumer.Poll()
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(messages))
	}
}

func TestConsumer_PollGroupModeWithAutoCommit(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	commitReceived := make(chan bool, 1)

	mb.onMessage(pb.MessageType_CONSUME_REQUEST, func(env *pb.MessageEnvelope) *pb.MessageEnvelope {
		var req pb.ConsumeRequest
		proto.Unmarshal(env.Payload, &req)

		if req.ConsumerGroup == nil || *req.ConsumerGroup == "" {
			t.Error("expected consumer group in group mode request")
		}

		resp := &pb.ConsumeResponse{
			Success: true,
			Messages: []*pb.StoredMessage{
				{Offset: 5, Topic: req.Topic, Payload: []byte("msg"), Timestamp: 1000, StoredAt: 1001},
			},
		}
		return &pb.MessageEnvelope{
			Type:    pb.MessageType_CONSUME_RESPONSE,
			Payload: mustMarshal(t, resp),
		}
	})

	mb.onMessage(pb.MessageType_COMMIT_OFFSET_REQUEST, func(env *pb.MessageEnvelope) *pb.MessageEnvelope {
		var req pb.CommitOffsetRequest
		proto.Unmarshal(env.Payload, &req)

		if req.Offset != 6 {
			t.Errorf("expected commit offset 6, got %d", req.Offset)
		}
		if req.ConsumerGroup != "test-group" {
			t.Errorf("expected group 'test-group', got %q", req.ConsumerGroup)
		}

		commitReceived <- true
		resp := &pb.CommitOffsetResponse{Success: true}
		return &pb.MessageEnvelope{
			Type:    pb.MessageType_COMMIT_OFFSET_RESPONSE,
			Payload: mustMarshal(t, resp),
		}
	})

	consumer, _ := NewConsumer(ConsumerConfig{
		BootstrapServers: mb.addr,
		ConsumerGroup:    "test-group",
		AutoCommit:       true,
		Logger:           testLogger,
	})
	defer consumer.Close()
	consumer.Connect()
	consumer.Subscribe("test-topic")

	messages, err := consumer.Poll()
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	select {
	case <-commitReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for auto-commit")
	}
}

func TestConsumer_ManualCommit(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	var committedOffset int64
	mb.onMessage(pb.MessageType_COMMIT_OFFSET_REQUEST, func(env *pb.MessageEnvelope) *pb.MessageEnvelope {
		var req pb.CommitOffsetRequest
		proto.Unmarshal(env.Payload, &req)
		committedOffset = req.Offset

		resp := &pb.CommitOffsetResponse{Success: true}
		return &pb.MessageEnvelope{
			Type:    pb.MessageType_COMMIT_OFFSET_RESPONSE,
			Payload: mustMarshal(t, resp),
		}
	})

	consumer, _ := NewConsumer(ConsumerConfig{
		BootstrapServers: mb.addr,
		ConsumerGroup:    "my-group",
		Logger:           testLogger,
	})
	defer consumer.Close()
	consumer.Connect()
	consumer.Subscribe("test-topic")

	if err := consumer.Commit("test-topic", 100); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if committedOffset != 100 {
		t.Errorf("expected committed offset 100, got %d", committedOffset)
	}
}

func TestConsumer_CommitFailsInSingleMode(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	consumer, _ := NewConsumer(ConsumerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})
	defer consumer.Close()
	consumer.Connect()

	err := consumer.Commit("test-topic", 100)
	if err == nil {
		t.Fatal("expected error when committing in single mode")
	}
}

func TestConsumer_Nack(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	mb.onMessage(pb.MessageType_NACK_REQUEST, func(env *pb.MessageEnvelope) *pb.MessageEnvelope {
		var req pb.NackRequest
		proto.Unmarshal(env.Payload, &req)

		resp := &pb.NackResponse{
			Success:     true,
			RoutedToDlq: true,
		}
		return &pb.MessageEnvelope{
			Type:    pb.MessageType_NACK_RESPONSE,
			Payload: mustMarshal(t, resp),
		}
	})

	consumer, _ := NewConsumer(ConsumerConfig{
		BootstrapServers: mb.addr,
		ConsumerGroup:    "my-group",
		Logger:           testLogger,
	})
	defer consumer.Close()
	consumer.Connect()
	consumer.Subscribe("test-topic")

	routedToDLQ, err := consumer.Nack("test-topic", 5)
	if err != nil {
		t.Fatalf("Nack: %v", err)
	}
	if !routedToDLQ {
		t.Error("expected routedToDLQ=true")
	}
}

func TestConsumer_NackFailsInSingleMode(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	consumer, _ := NewConsumer(ConsumerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})
	defer consumer.Close()
	consumer.Connect()

	_, err := consumer.Nack("test-topic", 0)
	if err == nil {
		t.Fatal("expected error when nacking in single mode")
	}
}

func TestConsumer_SeekByTime(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	mb.onMessage(pb.MessageType_SEARCH_OFFSET_BY_TIME_REQUEST, func(env *pb.MessageEnvelope) *pb.MessageEnvelope {
		var req pb.SearchOffsetByTimeRequest
		proto.Unmarshal(env.Payload, &req)

		resp := &pb.SearchOffsetByTimeResponse{
			Success: true,
			Offset:  42,
		}
		return &pb.MessageEnvelope{
			Type:    pb.MessageType_SEARCH_OFFSET_BY_TIME_RESPONSE,
			Payload: mustMarshal(t, resp),
		}
	})

	consumer, _ := NewConsumer(ConsumerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})
	defer consumer.Close()
	consumer.Connect()

	if err := consumer.SeekByTime("test-topic", 1609459200000); err != nil {
		t.Fatalf("SeekByTime: %v", err)
	}

	if consumer.CurrentOffset("test-topic") != 42 {
		t.Errorf("expected offset 42, got %d", consumer.CurrentOffset("test-topic"))
	}
}

func TestConsumer_PollConsumeError(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	mb.onMessage(pb.MessageType_CONSUME_REQUEST, func(env *pb.MessageEnvelope) *pb.MessageEnvelope {
		resp := &pb.ConsumeResponse{
			Success:      false,
			ErrorMessage: "topic not found",
		}
		return &pb.MessageEnvelope{
			Type:    pb.MessageType_CONSUME_RESPONSE,
			Payload: mustMarshal(t, resp),
		}
	})

	consumer, _ := NewConsumer(ConsumerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})
	defer consumer.Close()
	consumer.Connect()
	consumer.Subscribe("nonexistent-topic")

	_, err := consumer.Poll()
	if err == nil {
		t.Fatal("expected error for failed consume")
	}
}

func TestConsumer_MultipleBootstrapServers(t *testing.T) {
	mb1 := newMockBroker(t)
	defer mb1.close()
	mb2 := newMockBroker(t)
	defer mb2.close()

	bootstrapStr := fmt.Sprintf("%s,%s", mb1.addr, mb2.addr)

	consumer, err := NewConsumer(ConsumerConfig{
		BootstrapServers: bootstrapStr,
		Logger:           testLogger,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()

	if err := consumer.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
}
