#!/bin/bash

echo "🚀 Starting DRMQ Cluster in new terminal tabs..."

# Clean up any previously running instances
pkill -f "drmq-broker"
pkill -f "ConsumerApp"
pkill -f "ProducerApp"
pkill -f "load_test.sh"
pkill -f "vite"
sleep 2

# Clean previous data to start fresh (comment out if you want to keep old data)
# rm -rf data/node1 data/node2 data/node3
# echo "✓ Cleaned previous cluster state"

# Function to open a new tab and run a command
open_tab() {
    local title=$1
    local dir=$2
    local cmd=$3
    gnome-terminal --tab --title="$title" -- bash -c "cd $dir && echo -e '\033]0;$title\007' && $cmd; exec bash"
}

# Start Nodes
open_tab "DRMQ Node 1" "$(pwd)" "./mvnw -pl drmq-broker exec:java -Dexec.args=\"--node-id 1 --port 9092 --data-dir data/node1 --peers localhost:9093,localhost:9094\""
open_tab "DRMQ Node 2" "$(pwd)" "./mvnw -pl drmq-broker exec:java -Dexec.args=\"--node-id 2 --port 9093 --data-dir data/node2 --peers localhost:9092,localhost:9094\""
open_tab "DRMQ Node 3" "$(pwd)" "./mvnw -pl drmq-broker exec:java -Dexec.args=\"--node-id 3 --port 9094 --data-dir data/node3 --peers localhost:9092,localhost:9093\""

echo "⏳ Waiting 10 seconds for cluster to initialize and elect a leader..."
sleep 10

# Start Dashboard
open_tab "DRMQ Dashboard" "$(pwd)/drmq-dashboard" "VITE_USE_WEBSOCKET=true npm run dev"

# Start Consumer
open_tab "DRMQ Consumer" "$(pwd)/drmq-client" "echo -e 'subscribe load-test-topic\nstream' | mvn exec:java -Dexec.mainClass=\"com.drmq.client.commandLineExample.ConsumerApp\" -Dexec.args=\"localhost:9092,localhost:9093,localhost:9094 group-1\""

# Start Producer Load Test
open_tab "DRMQ Producer" "$(pwd)" "./load_test.sh"

echo "🎉 All services started in their own terminal tabs!"
echo "🔗 Open the Dashboard: http://localhost:5173"
