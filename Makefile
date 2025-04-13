
test: bench-test
	go test -timeout 30s -race .

bench-test:
	go test -timeout 30s -race -benchmem -bench=. -run ^$$ ./

bench:
	go test -timeout 30s -benchmem -bench=. -run ^$$ ./

lint:
	golangci-lint cache clean
	golangci-lint run