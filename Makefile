all: tidy test build

tidy:
	@echo "Tidying up..."
	@go mod tidy -v

test:
	@echo "Running unit tests..."
	@go test -cover ./...

build:
	@echo "Running go build..."
	@mkdir -p ./bin
	@go build -o bin/ ./...
