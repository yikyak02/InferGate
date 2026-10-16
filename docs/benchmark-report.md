# Simulation benchmark report

Measured locally on 15 October 2026 using native Go 1.27.2 executables on macOS/Apple Silicon. **These are synthetic serving measurements, not GPU, Ollama or vLLM performance measurements.** Each cell below comes from one trial; host timing affects results. There are no confidence intervals or claims of universal policy superiority.

## Workload and method

Every policy receives 400 offered requests, a nominal arrival rate of 120 requests/second, seed 42, and a burst of 20 coincident arrivals every 100 requests. The seeded trace contains 260 short and 140 long prompts, with 79 urgent requests. Each policy starts with three fresh simulated backends, so counters, active work and circuit state do not carry over.

Backends have synthetic capacities of 512, 1024 and 2048 token units; output delays of 2, 3 and 4 milliseconds/token; and 100 microseconds of prefill per actual input token. Each backend permits 32 concurrent requests. The gateway queue holds 128 requests, with a five-second queue deadline, a three-second starvation protection threshold and a 1.15 estimate multiplier. Readiness is established before traffic begins.

The simulator uses its own lexical input cost model, while the gateway estimates demand from prompt bytes plus the output cap. The round-robin and least-outstanding baselines share health checks, circuit breakers, concurrency limits, queueing and priorities, but omit per-backend token-budget gating. This evaluates the complete capacity-aware admission/routing policy against those baselines; it does not isolate each feature's individual contribution.

Latency percentiles below include **only completed requests**, including queue time. HTTP failures and interrupted streams are counted separately. The latency target is one second from the load generator starting an individual HTTP request through receipt of `done:true`. Goodput is the number of completions meeting that target divided by total run wall time, including drain. The arrival trace and launch lag are present in raw samples.

## Mixed traffic and bursts

| Policy | Completed / 400 | Completed within 1 s | Goodput / s | Completed p95, ms | HTTP failures | Interrupted streams |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Round-robin | 142 | 82 | 12.96 | 3734 | 258 | 0 |
| Least outstanding | 102 | 28 | 3.36 | 4600 | 298 | 0 |
| Memory-aware | 314 | 266 | 29.66 | 5339 | 86 | 0 |

The memory policy completed all 260 short requests, with 257 meeting the target. Ordinary short requests had a completed p95 of 189 ms. Long requests were much less successful: 54/140 completed and only 9 met the target. Its overall p95 was worse than both baselines because many long requests waited near the queue deadline before executing.

The memory policy's 86 HTTP failures were queue timeouts (503), with no recorded backend overload rejection (502). Round-robin produced 111 queue-full responses (429) and 147 backend rejection responses (502); least outstanding produced 130 and 168, respectively. This is evidence of improved backend overload protection and successful work under this workload, alongside a substantial long-request latency problem.

## Backend B crash and recovery

Backend B is crashed one second into the offered trace and recovers after one second. Its active requests terminate; the gateway does not replay them. Circuit cooldown can delay reuse beyond the simulator's recovery time.

| Policy | Completed / 400 | Completed within 1 s | Goodput / s | Completed p95, ms | HTTP failures | Interrupted streams |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Round-robin | 156 | 77 | 11.78 | 3167 | 243 | 1 |
| Least outstanding | 86 | 31 | 3.94 | 4358 | 314 | 0 |
| Memory-aware | 288 | 237 | 27.38 | 5431 | 110 | 2 |

The memory policy maintained more successful work and in-target completions in this trial, but two already-streaming requests failed. Its HTTP status count includes 290 responses with status 200; only 288 actually completed. Treating all 200 responses as successful would hide those failures.

The baseline counts also show why isolated trials should not be treated as deterministic performance rankings: round-robin completed more requests in this fault trial than in its no-fault trial. Circuit timing, host scheduling, and which expensive requests survive change the subsequent load. Repeat across seeds and trials before generalizing.

## Shorter actual outputs

The request trace and output caps remain identical, while each simulator generates 50% of the permitted output. The gateway still reserves against the full cap.

| Policy | Completed / 400 | Completed within 1 s | Goodput / s | Completed p95, ms | HTTP failures |
| --- | ---: | ---: | ---: | ---: | ---: |
| Round-robin | 248 | 201 | 32.57 | 2069 | 152 |
| Least outstanding | 207 | 165 | 26.61 | 3029 | 193 |
| Memory-aware | 367 | 277 | 32.37 | 5128 | 33 |

The memory policy completed more work, but its target goodput was slightly lower than round-robin's in this trial because it spent longer draining queued work. Conservative output-cap reservations can leave real capacity unused, and a long queue can increase eventual completions without improving every latency or throughput measure. Calibration and deadline-aware admission are useful next investigations.

## Correctness and operational checks

- `make test`: all race-enabled tests passed. Tests exercise concurrent capacity limits, queue bounds, cancellation, priorities and aging, stale telemetry, circuits, streaming/non-streaming behavior, Ollama request translation, failure recovery, metric isolation from slow scrapers and generator accounting.
- `make check`: `go vet` and formatting checks passed.
- Native executable smoke checks passed for streaming, non-streaming, 429 overload rejection, disconnect propagation, zero leaked backend use, and SIGTERM during active generation.
- All final recorded policy runs ended with zero gateway reservations and running-request gauges on every backend. No load-generator saturation or transport errors were recorded in these runs.
- JSON and YAML configuration parsing and shell syntax checks passed. Docker is not installed on the implementation host, so the container build, Prometheus rule evaluation and Grafana rendering were not exercised locally.
- The Ollama adapter was verified against an HTTP contract fixture. No installed Ollama service or NVIDIA backend was used for these measurements.

## Reproduce and inspect

```sh
make build test check
make smoke
./bin/experiment -requests 400 -rate 120 -output results/final/steady
./bin/experiment -requests 400 -rate 120 -fault-at 1s -fault-for 1s \
  -output results/final/failure
./bin/experiment -requests 400 -rate 120 -output-fraction 0.5 \
  -output results/final/shorter-outputs
```

Preserved summaries: [mixed traffic](evidence/steady.json), [backend failure](evidence/failure.json), [shorter outputs](evidence/shorter-outputs.json), and [native process checks](evidence/smoke.json). Full per-request JSONL samples, metrics and summaries are retained locally in `results/final/`; `results/` is ignored by version control. Future runs will vary even with identical input traces.

Before drawing production conclusions, run multiple seeds and repetitions on an otherwise idle host, vary the offered rate, test estimator underprediction as well as conservatism, compare stronger baselines such as capacity-weighted routing, and measure real model memory and service times separately. This implementation demonstrates useful reliability controls and a measurable synthetic tradeoff; it does not establish real-hardware memory safety or an optimal scheduler.
