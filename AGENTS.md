# Boxed Snake: AI Agents Architecture

This document outlines the architecture, roles, and interactions of the AI agents within the Boxed Snake project. The system is designed to execute dynamic task pipelines using a scripting language (Python) within a containerized Kubernetes (k8s) environment.

## 1. Client Agent
* **Role**: The primary entry point and interface for interaction with the Boxed Snake system.
* **Responsibilities**:
  * Submits dynamic task pipelines (e.g., Python scripts or workflow definitions) to the Orchestrator.
  * Monitors task execution status and progress.
  * Retrieves and presents final results or intermediate logs to the user or upstream systems.
  * Provides a seamless interface for defining complex AI-driven workflows.

## 2. Orchestrator Agent
* **Role**: The central control plane and brain of the system, residing within the k8s infrastructure.
* **Responsibilities**:
  * Receives task pipelines from the Client.
  * Parses, validates, and schedules the dynamic Python scripts.
  * Manages the lifecycle of Worker agents (spawning k8s pods as needed).
  * Handles load balancing, error recovery, retries, and state management.
  * Aggregates results from Workers and reports them back to the Client.

## 3. Worker Agents (The "Boxed Snakes")
* **Role**: The execution engines that run the dynamic pipelines in secure, isolated k8s containers.
* **Responsibilities**:
  * Execute assigned Python scripts in a controlled environment.
  * Process AI tasks, which may include data processing, model inference, tool execution, or other computational workloads.
  * Report telemetry, logs, and execution results back to the Orchestrator.
  * Ensure an isolated, reproducible, and secure execution environment for potentially untrusted code.

## System Interaction Flow
1. **Task Submission**: The Client formulates a pipeline and submits it to the Orchestrator.
2. **Scheduling & Dispatch**: The Orchestrator analyzes the pipeline, allocates resources, and provisions/assigns Worker(s) in the k8s cluster.
3. **Execution**: The Worker executes the dynamic Python script inside its containerized sandbox.
4. **Result Aggregation**: The Worker streams logs and final outputs back to the Orchestrator, which then finalizes the task and notifies the Client.
