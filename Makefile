.PHONY: help build test lint deploy clean crd example dry-run helm-install helm-uninstall helm-template

OPERATOR_IMAGE ?= self-healing-operator
OPERATOR_TAG ?= latest
KUBECTL ?= kubectl
NAMESPACE ?= self-healing-system
HELM ?= helm

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build Docker image
	docker build -t $(OPERATOR_IMAGE):$(OPERATOR_TAG) .

test: ## Run tests
	python -m pytest tests/ -v --tb=short

lint: ## Run linter and type checker
	python -m ruff check src/ tests/
	python -m mypy src/ --ignore-missing-imports

crd: ## Install CRD into cluster
	$(KUBECTL) apply -f deploy/crd.yaml

deploy: crd ## Deploy operator to cluster (CRD + RBAC + Deployment)
	$(KUBECTL) apply -f deploy/rbac.yaml
	$(KUBECTL) apply -f deploy/deployment.yaml
	@echo "✓ Operator deployed to namespace: $(NAMESPACE)"

undeploy: ## Remove operator from cluster
	$(KUBECTL) delete -f deploy/deployment.yaml --ignore-not-found
	$(KUBECTL) delete -f deploy/rbac.yaml --ignore-not-found
	$(KUBECTL) delete -f deploy/crd.yaml --ignore-not-found

example: ## Apply example AppHealth resource
	$(KUBECTL) apply -f examples/apphealth-payment-api.yaml

dry-run: ## Run operator locally in dry-run mode
	python -m kopf run --standalone -n default src/operator.py

status: ## Check operator status
	$(KUBECTL) get apphealths -A
	$(KUBECTL) get pods -n $(NAMESPACE) -l app=self-healing-operator

logs: ## Tail operator logs
	$(KUBECTL) logs -n $(NAMESPACE) -l app=self-healing-operator -f

clean: ## Remove build artifacts
	rm -rf build/ dist/ *.egg-info src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

check-prometheus: ## Test Prometheus connectivity
	python -m src.cli check

version: ## Print operator version
	@python -m src.cli version

helm-install: ## Install operator via Helm chart
	$(HELM) install self-healing-operator ./helm/self-healing-operator --namespace $(NAMESPACE) --create-namespace

helm-upgrade: ## Upgrade operator via Helm chart
	$(HELM) upgrade self-healing-operator ./helm/self-healing-operator --namespace $(NAMESPACE)

helm-uninstall: ## Uninstall operator Helm release
	$(HELM) uninstall self-healing-operator --namespace $(NAMESPACE)

helm-template: ## Render Helm templates locally (debug)
	$(HELM) template self-healing-operator ./helm/self-healing-operator --namespace $(NAMESPACE)
