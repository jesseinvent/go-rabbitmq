package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"

	_ "github.com/joho/godotenv/autoload"
)

type Comment struct {
	Text string `json:"text"`
}

func main() {
	r := gin.New()

	r.Use(gin.Logger())

	r.POST("/api/v1/comments", CreateComment)

	r.Run(":3000")
}

func CreateComment(c *gin.Context) {

	var comment Comment

	err := c.BindJSON(&comment)

	if err != nil {
		fmt.Printf("Error parsing body - %s", err)

		c.JSON(http.StatusBadRequest, gin.H{
			"status":  false,
			"message": "Invalid or empty fields supplied",
		})

		return

	}

	err = PushToQueue(comment.Text)

	if err != nil {
		fmt.Printf("Error pushing to queue - %s", err)

		c.JSON(http.StatusBadRequest, gin.H{
			"status":  false,
			"message": "Invalid or empty fields supplied",
		})

		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  true,
		"message": "Text successfully pushed to queue",
	})
}

func PushToQueue(body string) error {

	// Connect to RabbitMQ

	conn, err := amqp.Dial(os.Getenv("RABBIT_MQ_URL"))

	failOnError(err, "Failed to connect to RabbitMq")

	defer conn.Close()

	// Connect to a channel

	ch, err := conn.Channel()

	failOnError(err, "Failed to open a channel")

	defer ch.Close()

	// Open a queue

	q, err := ch.QueueDeclare("hello", false, false, false, false, nil)

	failOnError(err, "Failed to declare a queue")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	// Publish message to queue
	err = ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
	})

	failOnError(err, "Failed to publish a message to the queue")

	log.Printf("[x] Sent %s\n", body)

	return err
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
