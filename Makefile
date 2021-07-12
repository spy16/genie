all: tidy test

tidy:
	@echo "Tidying up..."
	@go mod tidy -v

test:
	@echo "Running unit tests..."
	@go test -cover ./...

build:
	@echo "Running go build..."
	@go build ./...
