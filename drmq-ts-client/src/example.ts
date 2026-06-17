import { DRMQProducer, DRMQConsumer } from './client';

async function main() {
  const servers = "localhost:9092,localhost:9093";
  
  console.log("--- Testing TS Producer ---");
  const producer = new DRMQProducer(servers);
  try {
    await producer.connect();
    const payload = Buffer.from("Hello from TypeScript!");
    const res = await producer.send("ts-topic", payload);
    if (res.success) {
      console.log(`Message sent successfully at offset ${res.offset}`);
    } else {
      console.log(`Failed to send: ${res.errorMessage}`);
    }
  } catch (err) {
    console.error(`Producer error: ${(err as Error).message}`);
  } finally {
    producer.close();
  }

  console.log("\n--- Testing TS Consumer ---");
  const consumer = new DRMQConsumer(servers, "ts-workers");
  try {
    await consumer.connect();
    await consumer.subscribe("ts-topic");
    
    console.log("Polling for messages...");
    const messages = await consumer.poll(10, 5000);
    for (const msg of messages) {
      console.log(`Received (offset ${msg.offset}): ${Buffer.from(msg.payload).toString('utf-8')}`);
    }
  } catch (err) {
    console.error(`Consumer error: ${(err as Error).message}`);
  } finally {
    consumer.close();
  }
}

main().catch(console.error);
