package drmq

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"

	pb "github.com/samuel025/DRMQ/drmq-go-client/protocol"
	"google.golang.org/protobuf/proto"
)

// mockBroker is a minimal TCP server that speaks the DRMQ wire protocol.
// Tests configure per-request handlers to control broker responses.
type mockBroker struct {
	listener net.Listener
	addr     string
	port     int

	mu       sync.Mutex
	handlers map[pb.MessageType]func(*pb.MessageEnvelope) *pb.MessageEnvelope
	conns    []net.Conn
	closed   bool
}

func newMockBroker(t *testing.T) *mockBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock broker: %v", err)
	}

	addr := ln.Addr().(*net.TCPAddr)
	mb := &mockBroker{
		listener: ln,
		addr:     fmt.Sprintf("127.0.0.1:%d", addr.Port),
		port:     addr.Port,
		handlers: make(map[pb.MessageType]func(*pb.MessageEnvelope) *pb.MessageEnvelope),
	}

	go mb.acceptLoop(t)
	return mb
}

func (mb *mockBroker) acceptLoop(t *testing.T) {
	for {
		conn, err := mb.listener.Accept()
		if err != nil {
			mb.mu.Lock()
			closed := mb.closed
			mb.mu.Unlock()
			if closed {
				return
			}
			continue
		}

		mb.mu.Lock()
		mb.conns = append(mb.conns, conn)
		mb.mu.Unlock()

		go mb.handleConn(t, conn)
	}
}

func (mb *mockBroker) handleConn(t *testing.T, conn net.Conn) {
	for {
		header := make([]byte, 4)
		_, err := io.ReadFull(conn, header)
		if err != nil {
			return
		}

		length := binary.BigEndian.Uint32(header)
		data := make([]byte, length)
		if _, err := io.ReadFull(conn, data); err != nil {
			return
		}

		var envelope pb.MessageEnvelope
		if err := proto.Unmarshal(data, &envelope); err != nil {
			t.Errorf("mock broker: unmarshal error: %v", err)
			return
		}

		mb.mu.Lock()
		handler, ok := mb.handlers[envelope.Type]
		mb.mu.Unlock()

		if !ok {
			t.Errorf("mock broker: no handler for message type %v", envelope.Type)
			return
		}

		resp := handler(&envelope)
		if resp == nil {
			continue
		}

		respData, err := proto.Marshal(resp)
		if err != nil {
			t.Errorf("mock broker: marshal response error: %v", err)
			return
		}

		respHeader := make([]byte, 4)
		binary.BigEndian.PutUint32(respHeader, uint32(len(respData)))
		if _, err := conn.Write(respHeader); err != nil {
			return
		}
		if _, err := conn.Write(respData); err != nil {
			return
		}
	}
}

func (mb *mockBroker) onMessage(msgType pb.MessageType, handler func(*pb.MessageEnvelope) *pb.MessageEnvelope) {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.handlers[msgType] = handler
}

func (mb *mockBroker) close() {
	mb.mu.Lock()
	mb.closed = true
	mb.mu.Unlock()

	mb.listener.Close()
	mb.mu.Lock()
	for _, conn := range mb.conns {
		conn.Close()
	}
	mb.mu.Unlock()
}

func mustMarshal(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	return data
}
