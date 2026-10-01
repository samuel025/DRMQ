.PHONY: proto build clean

# Regenerate protobuf Go code from the shared proto file
proto:
	protoc --go_out=protocol --go_opt=paths=source_relative -Iproto proto/messages.proto

build: proto
	go build ./...

clean:
	rm -f protocol/messages.pb.go

vet:
	go vet ./...

test:
	go test ./...
