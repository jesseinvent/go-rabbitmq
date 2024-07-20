## Example on Publishing and Subscribing to a RabbitMQ channel

### Run Rabbit container

```
 docker-compose up -d --build
```

### Install modules
```
go mod download
```

### Run Producer
```
go run producer/producer.go
```

### Run consumer
```
go  run consumer/consumer.go
```