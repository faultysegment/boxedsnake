# Kubernetes Infrastructure

This directory contains Helm charts, Kustomize overlays, or raw YAML manifests needed to deploy the Boxed Snake Orchestrator and related dependencies into a Kubernetes cluster.

### Dependencies
- **Redpanda**: The system requires a Redpanda cluster (or a single node for development) deployed in the cluster for Orchestrator-Worker messaging.
