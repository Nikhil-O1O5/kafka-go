proto-gen:
	@protoc --go_out=. --go_opt=paths=source_relative proto/orders.proto
	@mv proto/orders.pb.go internal/gen/orders/orders.pb.go

build-app:
	@go build -o ./bin/app ./cmd/.
	@chmod +x ./bin/app

app: build-app
	@./bin/app

test-app-race:
	@go clean -testcache
	@go test -race -v ./...
	