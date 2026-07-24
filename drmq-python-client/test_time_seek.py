import time
import logging
from drmq_client import DRMQProducer, DRMQConsumer

logging.basicConfig(level=logging.INFO)

def run_test():
    servers = "localhost:9092,localhost:9093,localhost:9094"
    topic = "time-seek-topic-" + str(int(time.time()))
    
    print(f"--- Starting Time-Based Seek Test on {topic} ---")
    producer = DRMQProducer(servers)
    try:
        producer.connect()
        
        # Produce 1st message
        producer.send(topic, b"Message 1 - Early").result()
        print("Sent Message 1")
        
        time.sleep(1)
        # Capture timestamp before Message 2
        target_time = int(time.time() * 1000)
        print(f"Captured target timestamp: {target_time}")
        time.sleep(1)
        
        # Produce 2nd and 3rd messages
        producer.send(topic, b"Message 2 - Target").result()
        print("Sent Message 2")
        producer.send(topic, b"Message 3 - Late").result()
        print("Sent Message 3")
        
    finally:
        producer.close()

    print("\n--- Starting Consumer ---")
    # Single mode consumer (no group_id)
    consumer = DRMQConsumer(servers, group_id=None)
    try:
        consumer.connect()
        
        print(f"Seeking to timestamp {target_time}...")
        consumer.seek_by_time(topic, target_time)
        print(f"Consumer local offsets: {consumer.local_offsets}")
        
        print("Polling...")
        messages = consumer.poll(max_messages=10, timeout_ms=2000)
        print(f"Polled {len(messages)} messages")
        
        if len(messages) == 0:
            print("TEST FAILED: No messages received.")
            # Let's see what is actually in the broker if we start from 0
            consumer.local_offsets[topic] = 0
            msgs_from_0 = consumer.poll(max_messages=100, timeout_ms=2000)
            print(f"Polled from 0: {len(msgs_from_0)} messages")
            for m in msgs_from_0:
                print(f" - offset={m.offset}, ts={m.timestamp}, payload={m.payload.decode('utf-8')}")
            exit(1)
            
        first_msg = messages[0].payload.decode('utf-8')
        print(f"First message received: {first_msg}")
        
        if "Message 2 - Target" in first_msg:
            print("TEST PASSED! Seek successfully skipped Message 1 and started at Message 2.")
        else:
            print(f"TEST FAILED! Expected Message 2, but got: {first_msg}")
            exit(1)
            
    finally:
        consumer.close()

if __name__ == "__main__":
    run_test()
