package drmq

import (
	"regexp"
	"testing"
)

func TestParseBootstrapServers_Single(t *testing.T) {
	servers, err := parseBootstrapServers("localhost:9092")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(servers) != 1 {
		t.Fatalf("expected 1 server, got %d", len(servers))
	}
	if servers[0].host != "localhost" || servers[0].port != 9092 {
		t.Errorf("got %s:%d, want localhost:9092", servers[0].host, servers[0].port)
	}
}

func TestParseBootstrapServers_Multiple(t *testing.T) {
	servers, err := parseBootstrapServers("host1:9092,host2:9093, host3:9094")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(servers) != 3 {
		t.Fatalf("expected 3 servers, got %d", len(servers))
	}
	expected := []struct {
		host string
		port int
	}{
		{"host1", 9092},
		{"host2", 9093},
		{"host3", 9094},
	}
	for i, exp := range expected {
		if servers[i].host != exp.host || servers[i].port != exp.port {
			t.Errorf("server[%d]: got %s:%d, want %s:%d",
				i, servers[i].host, servers[i].port, exp.host, exp.port)
		}
	}
}

func TestParseBootstrapServers_Empty(t *testing.T) {
	_, err := parseBootstrapServers("")
	if err == nil {
		t.Fatal("expected error for empty string")
	}
}

func TestParseBootstrapServers_InvalidFormat(t *testing.T) {
	_, err := parseBootstrapServers("nocolon")
	if err == nil {
		t.Fatal("expected error for missing colon")
	}
}

func TestParseBootstrapServers_InvalidPort(t *testing.T) {
	_, err := parseBootstrapServers("host:notanumber")
	if err == nil {
		t.Fatal("expected error for non-numeric port")
	}
}

func TestParseBootstrapServers_SkipsInvalidEntries(t *testing.T) {
	servers, err := parseBootstrapServers("good:1234,,bad,also:bad:format,ok:5678")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(servers) != 2 {
		t.Fatalf("expected 2 valid servers, got %d", len(servers))
	}
	if servers[0].host != "good" || servers[0].port != 1234 {
		t.Errorf("server[0]: got %s:%d", servers[0].host, servers[0].port)
	}
	if servers[1].host != "ok" || servers[1].port != 5678 {
		t.Errorf("server[1]: got %s:%d", servers[1].host, servers[1].port)
	}
}

func TestParseLeaderAddress_Valid(t *testing.T) {
	addr := parseLeaderAddress("NOT_LEADER:10.0.0.1:9092")
	if addr != "10.0.0.1:9092" {
		t.Errorf("got %q, want %q", addr, "10.0.0.1:9092")
	}
}

func TestParseLeaderAddress_Unknown(t *testing.T) {
	addr := parseLeaderAddress("NOT_LEADER:UNKNOWN")
	if addr != "" {
		t.Errorf("expected empty for UNKNOWN, got %q", addr)
	}
}

func TestParseLeaderAddress_Empty(t *testing.T) {
	addr := parseLeaderAddress("")
	if addr != "" {
		t.Errorf("expected empty for empty input, got %q", addr)
	}
}

func TestParseLeaderAddress_NoPrefix(t *testing.T) {
	addr := parseLeaderAddress("some other error")
	if addr != "" {
		t.Errorf("expected empty for non-NOT_LEADER msg, got %q", addr)
	}
}

func TestParseLeaderAddress_BadPort(t *testing.T) {
	addr := parseLeaderAddress("NOT_LEADER:host:abc")
	if addr != "" {
		t.Errorf("expected empty for bad port, got %q", addr)
	}
}

func TestParseLeaderAddress_NoPort(t *testing.T) {
	addr := parseLeaderAddress("NOT_LEADER:hostonly")
	if addr != "" {
		t.Errorf("expected empty for missing port, got %q", addr)
	}
}

func TestExtractLeaderAddress_Valid(t *testing.T) {
	addr := extractLeaderAddress("NOT_LEADER:10.0.0.1:9092")
	if addr != "10.0.0.1:9092" {
		t.Errorf("got %q, want %q", addr, "10.0.0.1:9092")
	}
}

func TestExtractLeaderAddress_Unknown(t *testing.T) {
	addr := extractLeaderAddress("NOT_LEADER:UNKNOWN")
	if addr != "" {
		t.Errorf("expected empty, got %q", addr)
	}
}

func TestExtractLeaderAddress_EmptyString(t *testing.T) {
	addr := extractLeaderAddress("")
	if addr != "" {
		t.Errorf("expected empty, got %q", addr)
	}
}

func TestExtractLeaderAddress_EmbeddedPrefix(t *testing.T) {
	addr := extractLeaderAddress("Error: NOT_LEADER:10.0.0.5:9094")
	if addr != "10.0.0.5:9094" {
		t.Errorf("got %q, want %q", addr, "10.0.0.5:9094")
	}
}

func TestGenerateID_Format(t *testing.T) {
	id := generateID()
	matched, err := regexp.MatchString(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, id)
	if err != nil {
		t.Fatalf("regex error: %v", err)
	}
	if !matched {
		t.Errorf("generated ID %q doesn't match UUID v4 format", id)
	}
}

func TestGenerateID_Unique(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := generateID()
		if ids[id] {
			t.Fatalf("duplicate ID generated: %s", id)
		}
		ids[id] = true
	}
}

func TestConsumedMessage_PayloadAsString(t *testing.T) {
	msg := ConsumedMessage{Payload: []byte("hello world")}
	if msg.PayloadAsString() != "hello world" {
		t.Errorf("got %q, want %q", msg.PayloadAsString(), "hello world")
	}
}

func TestSendResult_String(t *testing.T) {
	success := SendResult{Success: true, Offset: 42}
	if s := success.String(); s != "SendResult{success=true, offset=42}" {
		t.Errorf("got %q", s)
	}

	failure := SendResult{Success: false, ErrorMessage: "boom"}
	if s := failure.String(); s != "SendResult{success=false, error='boom'}" {
		t.Errorf("got %q", s)
	}
}
