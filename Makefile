.PHONY: db-up db-down test

db-up: ## Start the test database using Docker Compose
	@echo "Starting test database via Docker Compose..."
	docker-compose up -d --wait

db-down: ## Stop and remove the test database using Docker Compose
	@echo "Stopping test database via Docker Compose..."
	docker-compose down

test: ## Run tests against the test database
	@echo "Running tests..."
	go test -v -race ./...
