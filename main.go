package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
)

func main() {
	// get CLOUDAMQP_URL or use default
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	url := os.Getenv("CLOUDAMQP_URL")
	if url == "" {
		url = "amqp://localhost"
	}

	// establish connection
	connection, err := amqp.Dial(url)
	log.Println("Connected to RabbitMQ")
	if err != nil {
		log.Fatalln(url)
		panic(err)
	}
	defer connection.Close() // clean up connection when done

	// start a goroutine to consume messages
	go func() {
		// open a channel
		channel, err := connection.Channel()
		if err != nil {
			panic(err)
		}
		defer channel.Close() // clean up channel when done

		// declare a queue, set durable, autoDelete, exclusive, noWait to false
		durable, autoDelete, exclusive, noWait := false, true, false, false
		q, err := channel.QueueDeclare("test", durable, autoDelete, exclusive, noWait, nil)
		if err != nil {
			panic(err)
		}

		// bind queue to exchange with routing key "#"
		err = channel.QueueBind(q.Name, "#", "amq.topic", false, nil)
		if err != nil {
			panic(err)
		}

		// start consuming messages, set autoAck, exclusive, noLocal, noWait to false
		autoAck, exclusive, noLocal, noWait := false, false, false, false
		messages, err := channel.Consume(q.Name, "", autoAck, exclusive, noLocal, noWait, nil)
		if err != nil {
			panic(err)
		}

		// print received messages and acknowledge them
		// set multiAck to false
		multiAck := false
		for msg := range messages {
			fmt.Println("Body:", string(msg.Body), "Timestamp:", msg.Timestamp)
			msg.Ack(multiAck)
		}
	}()

	// start a goroutine to publish messages
	go func() {
		// open a channel
		channel, err := connection.Channel()
		if err != nil {
			panic(err)
		}

		// every second
		timer := time.NewTicker(1 * time.Second)
		for t := range timer.C {

			// require arg
			if len(os.Args) < 2 {
				fmt.Println("Please provide a number as an argument.")
				os.Exit(1)
			}

			// convert arg to int
			num, err := strconv.Atoi(os.Args[1])
			if err != nil {
				fmt.Println("The argument should be a number.")
				os.Exit(1)
			}

			// create message
			msg := amqp.Publishing{
				DeliveryMode: 1, // persistent
				Timestamp:    t,
				ContentType:  "text/plain",
				Body:         []byte(strconv.Itoa(num)),
			}

			// publish message to exchange "amq.topic" with routing key "ping"
			mandatory, immediate := false, false
			err = channel.Publish("amq.topic", "ping", mandatory, immediate, msg)
			if err != nil {
				panic(err)
			}
		}
	}()

	// block forever
	select {}
}
