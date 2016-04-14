

scheduler:
	go build -o mesos-redis main.go

executor:
	go build -o executor executor.go
