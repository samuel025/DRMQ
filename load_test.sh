#!/bin/bash

echo "🚀 Starting DRMQ High-Throughput Load Test..."
echo "Press Ctrl+C to stop the load test."
echo ""

cd drmq-client || exit

# Pipe an infinite loop of 'send' commands into the ProducerApp
while true; do
  # Generate a dummy 1KB payload
  PAYLOAD="DATA_PACKET_$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 1024 | head -n 1)"
  
  echo "send load-test-topic $PAYLOAD"
  
  # Wait 50 milliseconds between messages (approx 20 messages/sec)
  #sleep 0.05
done | mvn exec:java -Dexec.mainClass="com.drmq.client.commandLineExample.ProducerApp" -Dexec.args="localhost:9092,localhost:9093,localhost:9094"
