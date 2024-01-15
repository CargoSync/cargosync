all:
        go mod tidy
        go build -o client client/client.go
        go build -o server server/server.go
