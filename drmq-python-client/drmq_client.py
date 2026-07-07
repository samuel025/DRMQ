import socket
import struct
import time
import random
import threading
import concurrent.futures
import logging
from typing import List, Optional

logger = logging.getLogger(__name__)

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

        for _ in range(total_attempts):
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


class SendResult:
    def __init__(self, success: bool, offset: int = -1, error_message: str = ""):
        self.success = success
        self.offset = offset
        self.error_message = error_message

class PendingMessage:
    def __init__(self, payload: bytes, key: Optional[str], timestamp: int):
        self.payload = payload
        self.key = key
        self.timestamp = timestamp
        self.future = concurrent.futures.Future()

class DRMQProducer(DRMQClient):
    """Client for producing messages to the DRMQ cluster with asynchronous batching."""
    
    MAX_ACCUMULATOR_MESSAGES = 100000
    MAX_BATCH_MESSAGES = 10000
    MAX_PAYLOAD_BYTES = 10 * 1024 * 1024

    def __init__(self, bootstrap_servers: str):
        super().__init__(bootstrap_servers)
        self.accumulator = []
        self.accum_lock = threading.Lock()
        self.running = True
        self.send_thread = threading.Thread(target=self._sender_loop, daemon=True, name="drmq-producer-sender")
        self.send_thread.start()

    def send(self, topic: str, payload: bytes, key: Optional[str] = None) -> concurrent.futures.Future:
        """Enqueues a message for batching and returns a Future."""
        pm = PendingMessage(payload, key, int(time.time() * 1000))
        if not self.running:
            pm.future.set_exception(Exception("Producer is closed"))
            return pm.future

        with self.accum_lock:
            if len(self.accumulator) >= self.MAX_ACCUMULATOR_MESSAGES:
                pm.future.set_exception(Exception("Producer accumulator is full"))
                return pm.future
            self.accumulator.append((topic, pm))
        return pm.future
        
    def _sender_loop(self):
        while self.running:
            batches = {}
            with self.accum_lock:
                new_acc = []
                for topic, pm in self.accumulator:
                    if topic not in batches:
                        batches[topic] = {"msgs": [], "bytes": 0}
                    b = batches[topic]
                    if len(b["msgs"]) < self.MAX_BATCH_MESSAGES and b["bytes"] + len(pm.payload) <= self.MAX_PAYLOAD_BYTES:
                        b["msgs"].append(pm)
                        b["bytes"] += len(pm.payload)
                    else:
                        new_acc.append((topic, pm))
                self.accumulator = new_acc
            
            if not batches:
                time.sleep(0.005) # 5ms linger
                continue
                
            for topic, batch_data in batches.items():
                self._send_batch_with_retry(topic, batch_data["msgs"])

    def _send_batch_with_retry(self, topic: str, batch: List[PendingMessage]):
        try:
            req = pb.ProduceBatchRequest()
            req.topic = topic
            for pm in batch:
                entry = req.entries.add()
                entry.payload = pm.payload
                entry.client_timestamp = pm.timestamp
                if pm.key is not None:
                    entry.key = pm.key
                    
            envelope_bytes = req.SerializeToString()
        except Exception as e:
            for pm in batch:
                pm.future.set_exception(e)
            return

        delivery_timeout_ms = 120000
        start_ms = int(time.time() * 1000)
        current_backoff_ms = 100
        
        while True:
            if not self.running:
                for pm in batch:
                    pm.future.set_exception(Exception("Producer closed during retry"))
                return
                
            if int(time.time() * 1000) - start_ms > delivery_timeout_ms:
                break
                
            try:
                self._ensure_connected()
                # At-least-once semantics: If the network drops after the server processes the batch,
                # this retry may result in duplicate messages being appended.
                resp_payload = self._send_envelope(pb.MessageType.PRODUCE_BATCH_REQUEST, envelope_bytes)
                
                resp = pb.ProduceBatchResponse()
                resp.ParseFromString(resp_payload)
                
                if resp.success:
                    base_offset = resp.base_offset
                    for i, pm in enumerate(batch):
                        pm.future.set_result(SendResult(True, base_offset + i))
                    return
                else:
                    error_msg = resp.error_message
                    if error_msg and error_msg.startswith("NOT_LEADER:"):
                        leader_addr = error_msg[len("NOT_LEADER:"):]
                        if leader_addr != "UNKNOWN":
                            parts = leader_addr.split(":")
                            if len(parts) == 2:
                                self.host = parts[0]
                                self.port = int(parts[1])
                                super().close()
                                self._ensure_connected()
                                continue
                        super().close()
                        self._rotate_server()
                    elif error_msg and ("timed out" in error_msg or "Lost leadership" in error_msg or "Raft batch proposal" in error_msg):
                        super().close()
                        self._rotate_server()
                    else:
                        for pm in batch:
                            pm.future.set_result(SendResult(False, -1, error_msg))
                        return
            except Exception as e:
                logger.warning("Network error during batch send. Retrying may result in duplicate messages (at-least-once contract): %s", e)
                super().close()
                self._rotate_server()
                
            time.sleep(current_backoff_ms / 1000.0)
            current_backoff_ms = min(2000, current_backoff_ms * 2)
            
        for pm in batch:
            pm.future.set_exception(Exception("Failed to send batch: timeout exhausted"))

    def close(self):
        self.running = False
        if self.send_thread and self.send_thread.is_alive():
            self.send_thread.join(timeout=5.0)
            
        with self.accum_lock:
            for topic, pm in self.accumulator:
                pm.future.set_exception(Exception("Producer closed before send"))
            self.accumulator.clear()
            
        super().close()


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
        for _ in range(self.max_retries):
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
        for _ in range(self.max_retries):
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
        for _ in range(self.max_retries):
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

    def nack(self, topic: str, offset: int) -> bool:
        """
        Explicitly reject (NACK) an offset back to the broker.
        Returns True if the message was routed to the DLQ, False if it was requeued.
        """
        if not self.group_mode:
            raise RuntimeError("NACK is only supported in consumer group mode")
            
        for _ in range(self.max_retries):
            try:
                self._ensure_connected()
                req = pb.NackRequest()
                req.consumer_group = self.group_id or "single-mode-external"
                req.topic = topic
                req.offset = offset
                if self.group_mode:
                    req.consumer_id = self.consumer_id
                
                resp_payload = self._send_envelope(pb.MessageType.NACK_REQUEST, req.SerializeToString())
                resp = pb.NackResponse()
                resp.ParseFromString(resp_payload)
                
                if resp.success:
                    return resp.routed_to_dlq
                elif self._try_redirect_to_leader(resp.error_message):
                    continue
                else:
                    raise DRMQConnectionError(f"NACK failed: {resp.error_message}")
                    
            except Exception as e:
                if isinstance(e, DRMQConnectionError) and "NACK failed" in str(e):
                    raise
                self._reconnect()
        raise DRMQConnectionError(f"Failed to nack message after {self.max_retries} attempts")

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
