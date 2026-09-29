package drmq

import (
	"fmt"
	"testing"
	"time"

	pb "github.com/drmq/drmq-go-client/protocol"
	"google.golang.org/protobuf/proto"
)

func TestProducer_ConnectAndClose(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	producer, err := NewProducer(ProducerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}

	if err := producer.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	producer.Close()
}

func TestProducer_SendString(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	mb.onMessage(pb.MessageType_PRODUCE_BATCH_REQUEST, func(env *pb.MessageEnvelope) *pb.MessageEnvelope {
		var req pb.ProduceBatchRequest
		proto.Unmarshal(env.Payload, &req)

		if req.Topic != "test-topic" {
			t.Errorf("expected topic 'test-topic', got %q", req.Topic)
		}

		resp := &pb.ProduceBatchResponse{
			Success:    true,
			BaseOffset: 0,
			Count:      int32(len(req.Entries)),
		}
		return &pb.MessageEnvelope{
			Type:          pb.MessageType_PRODUCE_BATCH_RESPONSE,
			Payload:       mustMarshal(t, resp),
			CorrelationId: env.CorrelationId,
		}
	})

	producer, _ := NewProducer(ProducerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})
	defer producer.Close()
	producer.Connect()

	future := producer.SendString("test-topic", "hello world")
	result, err := future.GetWithTimeout(5 * time.Second)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if !result.Success {
		t.Fatalf("expected success, got error: %s", result.ErrorMessage)
	}
	if result.Offset != 0 {
		t.Errorf("expected offset 0, got %d", result.Offset)
	}
}

func TestProducer_SendWithKey(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	var receivedKey string
	mb.onMessage(pb.MessageType_PRODUCE_BATCH_REQUEST, func(env *pb.MessageEnvelope) *pb.MessageEnvelope {
		var req pb.ProduceBatchRequest
		proto.Unmarshal(env.Payload, &req)

		if len(req.Entries) > 0 && req.Entries[0].Key != nil {
			receivedKey = *req.Entries[0].Key
		}

		resp := &pb.ProduceBatchResponse{
			Success:    true,
			BaseOffset: 10,
			Count:      int32(len(req.Entries)),
		}
		return &pb.MessageEnvelope{
			Type:          pb.MessageType_PRODUCE_BATCH_RESPONSE,
			Payload:       mustMarshal(t, resp),
			CorrelationId: env.CorrelationId,
		}
	})

	producer, _ := NewProducer(ProducerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})
	defer producer.Close()
	producer.Connect()

	future := producer.SendWithKey("test-topic", []byte("data"), "user-123")
	result, err := future.GetWithTimeout(5 * time.Second)
	if err != nil {
		t.Fatalf("SendWithKey: %v", err)
	}
	if !result.Success {
		t.Fatalf("expected success")
	}
	if result.Offset != 10 {
		t.Errorf("expected offset 10, got %d", result.Offset)
	}
	if receivedKey != "user-123" {
		t.Errorf("expected key 'user-123', got %q", receivedKey)
	}
}

func TestProducer_SendMultipleMessages(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	mb.onMessage(pb.MessageType_PRODUCE_BATCH_REQUEST, func(env *pb.MessageEnvelope) *pb.MessageEnvelope {
		var req pb.ProduceBatchRequest
		proto.Unmarshal(env.Payload, &req)

		resp := &pb.ProduceBatchResponse{
			Success:    true,
			BaseOffset: 100,
			Count:      int32(len(req.Entries)),
		}
		return &pb.MessageEnvelope{
			Type:          pb.MessageType_PRODUCE_BATCH_RESPONSE,
			Payload:       mustMarshal(t, resp),
			CorrelationId: env.CorrelationId,
		}
	})

	producer, _ := NewProducer(ProducerConfig{
		BootstrapServers: mb.addr,
		LingerMs:         50,
		Logger:           testLogger,
	})
	defer producer.Close()
	producer.Connect()

	futures := make([]*SendFuture, 5)
	for i := 0; i < 5; i++ {
		futures[i] = producer.SendString("test-topic", "msg")
	}

	for i, f := range futures {
		result, err := f.GetWithTimeout(5 * time.Second)
		if err != nil {
			t.Fatalf("msg[%d]: %v", i, err)
		}
		if !result.Success {
			t.Errorf("msg[%d]: expected success", i)
		}
	}
}

func TestProducer_SendAtomic(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	mb.onMessage(pb.MessageType_ATOMIC_PRODUCE_REQUEST, func(env *pb.MessageEnvelope) *pb.MessageEnvelope {
		var req pb.AtomicProduceRequest
		proto.Unmarshal(env.Payload, &req)

		if len(req.Slices) < 2 {
			t.Errorf("expected at least 2 slices, got %d", len(req.Slices))
		}

		baseOffsets := make(map[string]int64)
		for _, slice := range req.Slices {
			baseOffsets[slice.Topic] = 50
		}

		resp := &pb.AtomicProduceResponse{
			Success:     true,
			BaseOffsets: baseOffsets,
		}
		return &pb.MessageEnvelope{
			Type:          pb.MessageType_ATOMIC_PRODUCE_RESPONSE,
			Payload:       mustMarshal(t, resp),
			CorrelationId: env.CorrelationId,
		}
	})

	producer, _ := NewProducer(ProducerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})
	defer producer.Close()
	producer.Connect()

	future := producer.SendAtomic(map[string][]byte{
		"orders":   []byte(`{"id":"1"}`),
		"payments": []byte(`{"id":"2"}`),
	})

	offsets, err := future.GetWithTimeout(5 * time.Second)
	if err != nil {
		t.Fatalf("SendAtomic: %v", err)
	}

	if offsets["orders"] != 50 {
		t.Errorf("orders offset: got %d, want 50", offsets["orders"])
	}
	if offsets["payments"] != 50 {
		t.Errorf("payments offset: got %d, want 50", offsets["payments"])
	}
}

func TestProducer_SendAtomicRequiresAtLeast2Topics(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	producer, _ := NewProducer(ProducerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})
	defer producer.Close()

	future := producer.SendAtomic(map[string][]byte{
		"only-one": []byte("data"),
	})

	_, err := future.GetWithTimeout(1 * time.Second)
	if err == nil {
		t.Fatal("expected error for single-topic atomic send")
	}
}

func TestProducer_PayloadTooLarge(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	producer, _ := NewProducer(ProducerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})
	defer producer.Close()

	bigPayload := make([]byte, 11*1024*1024)
	future := producer.Send("test-topic", bigPayload)

	_, err := future.GetWithTimeout(1 * time.Second)
	if err == nil {
		t.Fatal("expected error for oversized payload")
	}
}

func TestProducer_ProduceError(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	mb.onMessage(pb.MessageType_PRODUCE_BATCH_REQUEST, func(env *pb.MessageEnvelope) *pb.MessageEnvelope {
		resp := &pb.ProduceBatchResponse{
			Success:      false,
			ErrorMessage: "disk full",
			ErrorCode:    pb.ErrorCode_UNKNOWN_ERROR,
		}
		return &pb.MessageEnvelope{
			Type:          pb.MessageType_PRODUCE_BATCH_RESPONSE,
			Payload:       mustMarshal(t, resp),
			CorrelationId: env.CorrelationId,
		}
	})

	producer, _ := NewProducer(ProducerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})
	defer producer.Close()
	producer.Connect()

	future := producer.SendString("test-topic", "test")
	_, err := future.GetWithTimeout(5 * time.Second)
	if err == nil {
		t.Fatal("expected error from broker")
	}
}

func TestProducer_IsConnected(t *testing.T) {
	mb := newMockBroker(t)
	defer mb.close()

	producer, _ := NewProducer(ProducerConfig{
		BootstrapServers: mb.addr,
		Logger:           testLogger,
	})

	if producer.IsConnected() {
		t.Error("should not be connected before Connect()")
	}

	producer.Connect()
	if !producer.IsConnected() {
		t.Error("should be connected after Connect()")
	}

	producer.Close()
}

func TestSendFuture_Get(t *testing.T) {
	f := newSendFuture()
	go func() {
		time.Sleep(50 * time.Millisecond)
		f.complete(SendResult{Success: true, Offset: 99})
	}()

	result, err := f.Get()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if result.Offset != 99 {
		t.Errorf("expected offset 99, got %d", result.Offset)
	}
}

func TestSendFuture_GetWithTimeout_Success(t *testing.T) {
	f := newSendFuture()
	go func() {
		time.Sleep(10 * time.Millisecond)
		f.complete(SendResult{Success: true, Offset: 7})
	}()

	result, err := f.GetWithTimeout(1 * time.Second)
	if err != nil {
		t.Fatalf("GetWithTimeout: %v", err)
	}
	if result.Offset != 7 {
		t.Errorf("expected offset 7, got %d", result.Offset)
	}
}

func TestSendFuture_GetWithTimeout_Timeout(t *testing.T) {
	f := newSendFuture()

	_, err := f.GetWithTimeout(50 * time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestSendFuture_Fail(t *testing.T) {
	f := newSendFuture()
	go func() {
		f.fail(fmt.Errorf("something broke"))
	}()

	_, err := f.Get()
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "something broke" {
		t.Errorf("got error %q", err)
	}
}

func TestAtomicSendFuture_Get(t *testing.T) {
	f := newAtomicSendFuture()
	go func() {
		f.complete(map[string]int64{"topic-a": 1, "topic-b": 2})
	}()

	offsets, err := f.Get()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if offsets["topic-a"] != 1 || offsets["topic-b"] != 2 {
		t.Errorf("unexpected offsets: %v", offsets)
	}
}

func TestAtomicSendFuture_GetWithTimeout_Timeout(t *testing.T) {
	f := newAtomicSendFuture()
	_, err := f.GetWithTimeout(50 * time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestIsNotLeaderError(t *testing.T) {
	if !isNotLeaderError(pb.ErrorCode_NOT_LEADER, "") {
		t.Error("should detect NOT_LEADER error code")
	}
	if !isNotLeaderError(pb.ErrorCode_NONE, "NOT_LEADER:host:9092") {
		t.Error("should detect NOT_LEADER in message")
	}
	if isNotLeaderError(pb.ErrorCode_NONE, "some other error") {
		t.Error("should not flag non-NOT_LEADER errors")
	}
	if isNotLeaderError(pb.ErrorCode_UNKNOWN_ERROR, "disk full") {
		t.Error("should not flag UNKNOWN_ERROR without NOT_LEADER")
	}
}
