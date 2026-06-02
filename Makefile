.PHONY: cluster docker-build docker-load deploy run clean all proto

CLUSTER_NAME = boxedsnake-cluster
KIND = $$(go env GOPATH)/bin/kind

all: cluster docker-build docker-load deploy

proto:
	buf generate

cluster:
	@echo "Checking if Kind cluster exists..."
	@if ! $(KIND) get clusters | grep -q "^$(CLUSTER_NAME)$$"; then \
		echo "Creating Kind cluster..."; \
		$(KIND) create cluster --name $(CLUSTER_NAME); \
	else \
		echo "Cluster $(CLUSTER_NAME) already exists. Skipping creation."; \
	fi

docker-build:
	@echo "Building Docker images..."
	docker build -t boxedsnake-orchestrator:latest -f cmd/orchestrator/Dockerfile .
	cd workers && docker build -t boxedsnake-worker:latest -f Dockerfile .

docker-load:
	@echo "Loading images into Kind cluster..."
	$(KIND) load docker-image boxedsnake-orchestrator:latest --name $(CLUSTER_NAME)
	$(KIND) load docker-image boxedsnake-worker:latest --name $(CLUSTER_NAME)

deploy:
	@echo "Deploying to Kubernetes..."
	kubectl apply -f deployments/k8s/
	@echo "Waiting for pods to be ready..."
	kubectl wait --for=condition=ready pod -l app=redpanda --timeout=120s || true
	kubectl wait --for=condition=ready pod -l app=orchestrator --timeout=120s || true
	kubectl wait --for=condition=ready pod -l app=worker --timeout=120s || true

run:
	@echo "Starting port-forward in background..."
	kubectl port-forward svc/orchestrator 8080:8080 > /dev/null 2>&1 & \
	PID=$$!; \
	sleep 2; \
	go run ./cmd/client; \
	kill $$PID

clean:
	@echo "Deleting Kind cluster..."
	kind delete cluster --name $(CLUSTER_NAME)
