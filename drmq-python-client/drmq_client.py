import socket
import struct
import time
import random
from typing import List, Optional

# Import the generated Protobuf classes
import messages_pb2 as pb

class DRMQConnectionError(Exception):
    """Raised when the client cannot connect to the broker."""
    pass

class DRMQClient:
    """Base class for TCP connection and Protobuf framing."""
    
    def __init__(self, bootstrap_servers: str):
        # bootstrap_servers is a comma-separated string like "localhost:9092,localhost:9093"
        self.bootstrap_servers = [s.strip().split(':') for s in bootstrap_servers.split(',')]
        self.current_server_index = random.randint(0, len(self.bootstrap_servers) - 1)
        self.host = self.bootstrap_servers[self.current_server_index][0]
        self.port = int(self.bootstrap_servers[self.current_server_index][1])
        self.sock: Optional[socket.socket] = None
        self.max_retries = 5

    def connect(self):
        self._ensure_connected()

    def _ensure_connected(self):
        if self.sock is not None:
            return

        last_exception = None
        total_attempts = self.max_retries * max(1, len(self.bootstrap_servers))

        for attempt in range(total_attempts):
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.settimeout(5.0)
                self.sock.connect((self.host, self.port))
                return
            except Exception as e:
                last_exception = e
                self.close()
                self._rotate_server()
                time.sleep(0.5)
                    
        raise DRMQConnectionError(f"Failed to connect after {total_attempts} attempts. Last error: {last_exception}")

    def _rotate_server(self):
        if len(self.bootstrap_servers) <= 1:
            return
        self.current_server_index = (self.current_server_index + 1) % len(self.bootstrap_servers)
        self.host = self.bootstrap_servers[self.current_server_index][0]
        self.port = int(self.bootstrap_servers[self.current_server_index][1])

    def _reconnect(self):
        self.close()
        self._rotate_server()
        self._ensure_connected()

    def _try_redirect_to_leader(self, error_message: str) -> bool:
        if not error_message or not error_message.startswith("NOT_LEADER:"):
            return False
        
        leader_info = error_message[len("NOT_LEADER:"):]
        if leader_info != "UNKNOWN":
            parts = leader_info.split(":")
            if len(parts) == 2:
                self.host = parts[0]
                self.port = int(parts[1])
                self.close()
                self._ensure_connected()
                return True
        
        self._reconnect()
        return True

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def _send_envelope(self, msg_type: int, payload_bytes: bytes) -> bytes:
        """Wraps a payload in an envelope, frames it, and sends it, returning the raw response bytes."""
        self._ensure_connected()

        # 1. Create the MessageEnvelope
        envelope = pb.MessageEnvelope()
        envelope.type = msg_type
        envelope.payload = payload_bytes
        envelope_bytes = envelope.SerializeToString()

        # 2. Add the 4-byte Big-Endian length prefix
        length_prefix = struct.pack('>I', len(envelope_bytes))
        
        # Send Length + Data
        self.sock.sendall(length_prefix + envelope_bytes)

        # 3. Read the response length prefix (4 bytes)
        resp_len_bytes = self._recv_exactly(4)
        if not resp_len_bytes:
            raise ConnectionError("Broker closed connection")
        resp_len = struct.unpack('>I', resp_len_bytes)[0]

        # 4. Read the response envelope
        resp_envelope_bytes = self._recv_exactly(resp_len)
        resp_envelope = pb.MessageEnvelope()
        resp_envelope.ParseFromString(resp_envelope_bytes)
        
        return resp_envelope.payload

    def _recv_exactly(self, n: int) -> bytes:
        """Helper to read exactly n bytes from the TCP stream."""
        data = bytearray()
        while len(data) < n:
            packet = self.sock.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return bytes(data)


class DRMQProducer(DRMQClient):
    """Client for producing messages to the DRMQ cluster."""
    
    def send(self, topic: str, payload: bytes, key: Optional[str] = None) -> pb.ProduceResponse:
        """Sends a message to a topic with automatic retries and leader redirect handling."""
        req = pb.ProduceRequest()
        req.topic = topic
        req.payload = payload
        if key:
            req.key = key
        req.timestamp = int(time.time() * 1000)

        for attempt in range(self.max_retries):
            try:
                self._ensure_connected()
                resp_payload = self._send_envelope(pb.MessageType.PRODUCE_REQUEST, req.SerializeToString())
                
                resp = pb.ProduceResponse()
                resp.ParseFromString(resp_payload)

                if not resp.success and self._try_redirect_to_leader(resp.error_message):
                    continue # Retry on new leader

                return resp
            except Exception as e:
                self._reconnect()

        raise DRMQConnectionError(f"Failed to send message after {self.max_retries} attempts")


class DRMQConsumer(DRMQClient):
    """Client for consuming messages from the DRMQ cluster."""
    
    def __init__(self, bootstrap_servers: str, group_id: Optional[str] = None, consumer_id: str = "py-consumer-1"):
        super().__init__(bootstrap_servers)
        self.group_id = group_id
        self.consumer_id = consumer_id
        self.subscriptions = []
        self.local_offsets = {}
        self.auto_commit = False
        self.group_mode = group_id is not None

    def subscribe(self, topic: str, from_offset: Optional[int] = None):
        """Subscribe to a topic. Fetches committed offset from broker if needed."""
        if topic not in self.subscriptions:
            self.subscriptions.append(topic)

        if self.group_mode:
            self.local_offsets[topic] = -1 # Broker manages it
        else:
            if from_offset is not None:
                self.local_offsets[topic] = from_offset
            else:
                self.local_offsets[topic] = self._fetch_offset(topic)

    def poll(self, max_messages: int = 100, timeout_ms: int = 1000) -> List[pb.StoredMessage]:
        """Poll the broker for new messages across all subscribed topics."""
        for attempt in range(self.max_retries):
            try:
                self._ensure_connected()
                all_messages = []
                
                for topic in self.subscriptions:
                    req = pb.ConsumeRequest()
                    req.topic = topic
                    req.max_messages = max_messages
                    req.timeout_ms = timeout_ms
                    
                    if self.group_mode:
                        req.consumer_group = self.group_id
                        req.consumer_id = self.consumer_id
                    else:
                        req.from_offset = self.local_offsets.get(topic, 0)

                    resp_payload = self._send_envelope(pb.MessageType.CONSUME_REQUEST, req.SerializeToString())
                    resp = pb.ConsumeResponse()
                    resp.ParseFromString(resp_payload)
                    
                    if resp.success:
                        all_messages.extend(resp.messages)
                        if resp.messages:
                            next_offset = resp.messages[-1].offset + 1
                            self.local_offsets[topic] = next_offset
                            if self.auto_commit:
                                self.commit(topic, next_offset)
                    elif self._try_redirect_to_leader(resp.error_message):
                        # Break and retry the entire poll loop on the new leader
                        raise ConnectionError("Redirected to leader")

                return all_messages
            except Exception:
                self._reconnect()
                
        raise DRMQConnectionError(f"Failed to poll messages after {self.max_retries} attempts")

    def _fetch_offset(self, topic: str) -> int:
        for attempt in range(self.max_retries):
            try:
                self._ensure_connected()
                req = pb.FetchOffsetRequest()
                req.consumer_group = self.group_id or "single-mode-external"
                req.topic = topic
                
                resp_payload = self._send_envelope(pb.MessageType.FETCH_OFFSET_REQUEST, req.SerializeToString())
                resp = pb.FetchOffsetResponse()
                resp.ParseFromString(resp_payload)
                
                if not resp.success and self._try_redirect_to_leader(resp.error_message):
                    continue

                return max(0, resp.offset)
            except Exception:
                self._reconnect()
                
        return 0

    def commit(self, topic: str, offset: int):
        """Commit an offset back to the broker."""
        for attempt in range(self.max_retries):
            try:
                self._ensure_connected()
                req = pb.CommitOffsetRequest()
                req.consumer_group = self.group_id or "single-mode-external"
                req.topic = topic
                req.offset = offset
                if self.group_mode:
                    req.consumer_id = self.consumer_id
                
                resp_payload = self._send_envelope(pb.MessageType.COMMIT_OFFSET_REQUEST, req.SerializeToString())
                resp = pb.CommitOffsetResponse()
                resp.ParseFromString(resp_payload)
                
                if resp.success:
                    self.local_offsets[topic] = offset
                    return
                elif self._try_redirect_to_leader(resp.error_message):
                    continue
                    
            except Exception:
                self._reconnect()

# ==========================================
# Example Usage 
# ==========================================
if __name__ == "__main__":
    servers = "localhost:9092,localhost:9093"
    
    # 1. Producer Example
    print("--- Testing Producer ---")
    producer = DRMQProducer(servers)
    try:
        producer.connect()
        res = producer.send("python-topic", b"Hello from Python!")
        if res.success:
            print(f"Message sent successfully at offset {res.offset}")
        else:
            print(f"Failed to send: {res.error_message}")
    except Exception as e:
        print(f"Producer error: {e}")
    finally:
        producer.close()

    # 2. Consumer Example (Group Mode)
    print("\n--- Testing Consumer ---")
    consumer = DRMQConsumer(servers, group_id="python-workers")
    consumer.auto_commit = True  # Enable auto-commit!
    try:
        consumer.connect()
        consumer.subscribe("python-topic")
        
        print("Polling for messages...")
        messages = consumer.poll(max_messages=10, timeout_ms=5000)
        for msg in messages:
            print(f"Received (offset {msg.offset}): {msg.payload.decode('utf-8')}")
            
    except Exception as e:
        print(f"Consumer error: {e}")
    finally:
        consumer.close()
