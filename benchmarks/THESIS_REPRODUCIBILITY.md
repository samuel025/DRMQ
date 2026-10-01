# Thesis Figures Reproducibility

The following scripts were specifically created to generate the experimental data used in the thesis figures (Figures 1-6). They orchestrate the Docker environments, compile the Java client, execute the `AtomicStressTestApp` under specific load constraints, and output `.csv` results.

**Methodological Note:** To ensure a fair comparison against Kafka's Two-Phase Commit architecture, Figures 1-5 utilize a **Closed-Loop Workload Model**. In these benchmarks, the application-level concurrency is strictly bound (`pendingTxnLimit = Concurrency`), forcing threads to wait synchronously for a transaction acknowledgment before sending the next. Figure 6 uncaps this limit to demonstrate absolute maximum asynchronous throughput.

**Note on Comparative Baselines:** To guarantee reproducibility of the comparative graphs, the scripts for Figures 1, 2, and 4 are completely self-contained. When executed, they automatically orchestrate the DRMQ cluster, run the DRMQ workload, tear it down, and then orchestrate a pristine Kafka KRaft cluster to run the exact same closed-loop workload. Both results are output to the same CSV for direct comparison.

### `run_figure1_benchmark.sh` (Serial Throughput)
- **Goal:** Establishes the baseline serial throughput of DRMQ vs. Kafka without lock contention.
- **Config:** Concurrency = 1, `pendingTxnLimit = 1` (Strict Synchronous), Mode = `separate`.
- **Reproduce:** `./run_figure1_benchmark.sh`

### `run_figure2_benchmark.sh` (Throughput vs. Concurrency)
- **Goal:** Demonstrates how throughput scales (or plateaus) as concurrency increases from 1 to 50 threads.
- **Config:** Concurrencies = 1, 5, 10, 20, 50. `pendingTxnLimit = C` (Strict Synchronous), Mode = `separate` (mimicking Kafka's 1-producer-per-thread limitation).
- **Reproduce:** `./run_figure2_benchmark.sh`

### `run_figure3_benchmark.sh` (Throughput vs. Topic Fan-out)
- **Goal:** Proves graceful degradation of DRMQ's single-round consensus as the number of topics per transaction increases from 2 to 10.
- **Config:** Topics = 2, 3, 5, 10. `pendingTxnLimit = C` (Strict Synchronous), Mode = `separate`.
- **Reproduce:** `./run_figure3_benchmark.sh`

### `run_figure4_benchmark.sh` (Tail Latency Distribution)
- **Goal:** Isolates the commit latency distribution ($p_{50}$ to $p_{99.9}$) in serial mode to highlight DRMQ's lack of dual-I/O overhead.
- **Config:** Concurrency = 1, `pendingTxnLimit = 1` (Strict Synchronous), Mode = `separate`.
- **Reproduce:** `./run_figure4_benchmark.sh`

### `run_figure5_deepdive.sh` (Batching Efficiency Deep Dive)
- **Goal:** Proves DRMQ's cross-thread shared batching efficiency. Keeps the strict synchronous application limits, but switches to `shared` mode with a larger batch threshold.
- **Config:** Concurrencies = 1, 4, 8, 16, 32, 50. `pendingTxnLimit = C` (Strict Synchronous), Mode = `shared`, Linger = 5ms. Compares 16 KB vs 1 MB batch sizes.
- **Reproduce:** `./run_figure5_deepdive.sh`

### `run_figure6_maxpower.sh` (Peak Asynchronous Throughput)
- **Goal:** Unrestrained workload demonstrating DRMQ's absolute theoretical maximum performance.
- **Config:** Concurrencies = 1, 10, 20, 50. `pendingTxnLimit = 5000` (Asynchronous Pipelining), Mode = `shared`, Linger = 5ms, Batch Size = 1 MB.
- **Reproduce:** `./run_figure6_maxpower.sh`
