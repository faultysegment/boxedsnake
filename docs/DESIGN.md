# Boxed Snake - Design Documentation

## 1. System Architecture

The project consists of three primary components. The Client and Orchestrator communicate via `connect-go` (Protobuf over HTTP/2).

1. **Client**: A Go application featuring an interactive TUI (Terminal User Interface) built with Bubble Tea. It allows users to write and submit Python task pipelines. (Located in `cmd/client`)
2. **Orchestrator**: A Go Kubernetes service that acts as an API Gateway. It receives tasks from the client and publishes them to the message broker. (Located in `cmd/orchestrator`)
3. **Worker**: Pods spun up dynamically (or running as a pool) to execute Python scripts in isolation. (Located in `workers/`)

The architecture is modeled using `likec4` (see [architecture.c4](architecture.c4) for the raw model).

## 2. Kubernetes Integration

* The entire stack is fully containerized and designed to run in Kubernetes.
* We provide a `Makefile` that automatically provisions a local `Kind` cluster, builds the Docker images, and deploys the manifests located in `deployments/k8s/`.
* The **Orchestrator** runs as a standard Deployment and exposes a Service.
* The **Workers** run as a separate Deployment (a pool of workers). They are persistent and consume tasks continuously from Redpanda.
* **Redpanda** is deployed as a single-replica StatefulSet/Deployment for local testing to act as the Kafka-compatible message broker.

## 3. Communication

The system utilizes an event-driven dual-protocol approach:

### Client to Orchestrator (`connect-go`)
* The synchronous user-facing API contracts are defined using Protocol Buffers in `api/`.
* The Client uses `connect-go` to call the Orchestrator's `ExecuteTask` RPC (supporting gRPC and Connect over HTTP/2).

### Orchestrator to Worker (Redpanda / Kafka)
* The Orchestrator acts as a **Producer**, packaging the Python script and metadata into a JSON message and publishing it to the `tasks` topic.
* The pool of Workers act as **Consumers**, reading from the `tasks` topic.
* Upon completion, the Workers publish the logs, status, and final JSON results back to a `task-results` topic.
* The Orchestrator consumes from `task-results` to update state and return the synchronous RPC response back to the Client.

## 4. Script Execution Structure

Scripts submitted to Boxed Snake are strictly structured. Users do not write top-level execution code. Instead, scripts must define:
1. `def run():` - The main entry point for the logic. It must return the exact string `"ok"` upon success.
2. `def send_result():` - Called automatically by the worker only if `run()` succeeded. Responsible for writing output to `BOXED_SNAKE_OUTPUT_FILE`.

The Worker dynamically wraps the user's script in an execution block before running it via a `subprocess`, ensuring proper flow control and error handling.

## 5. Security & Isolation

* **Worker Isolation**: Since workers execute untrusted scripts, they sandbox the Python execution environment internally using `subprocess` with strict timeouts. Future iterations should leverage `cgroups`, unprivileged users, or `gVisor`.
* Network access for the worker pods is restricted strictly to the Redpanda broker.
