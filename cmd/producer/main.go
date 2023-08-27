package main

import (
	"context"
	"fmt"
	"rabbitmq_testing/internal"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := internal.ConnectToRabbitMQ("testmest", "123", "localhost:5672", "customers")
	if err != nil {
		panic(fmt.Sprintf("RabbitMQ connection failed: %v", err))
	}

	defer conn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(fmt.Sprintf("RabbitMQ failed to create a channel: %v", err))
	}

	defer func (client internal.RabbitClient) {
		if err:= client.Close(); err != nil {
			fmt.Printf("RabbitMQ failed to close the channel: %v\n", err)
		}		
	}(client)

	// direct exchange (replyConnection)
	consumerConn, err := internal.ConnectToRabbitMQ("testmest", "123", "localhost:5672", "customers")
	if err != nil {
		panic(fmt.Sprintf("RabbitMQ connection failed: %v", err))
	}

	defer conn.Close()
	
	// direct exchange (replyClient)
	consumerClient, err := internal.NewRabbitMQClient(consumerConn)
	if err != nil {
		panic(fmt.Sprintf("RabbitMQ failed to create a channel: %v", err))
	}

	// direct exchange (replyClient)
	defer func (consumerClient internal.RabbitClient) {
		if err:= consumerClient.Close(); err != nil {
			fmt.Printf("RabbitMQ failed to close the channel: %v\n", err)
		}		
	}(consumerClient)

	// direct exchange queue
	q, err := consumerClient.CreateQueue("", true, false)
	if err != nil {
		panic(fmt.Sprintf("RabbitMQ failed to create a queue(reply queue): %v", err))
	}

	// direct exchange binding
	if err := consumerClient.CreateBinding(q.Name, q.Name, "customer_events_direct"); err != nil {
		panic(fmt.Sprintf("RabbitMQ failed to bind a queue to an exchange: %v", err))
	}
	
	// direct exchange reader
	messages, err := consumerClient.Read(q.Name)
	if err != nil {
		panic(fmt.Sprintf("RabbitMQ failed to read reply messages: %v", err))
	}

	go func() {
		for message := range messages {
			fmt.Println("New message reply: ", string(message.Body))
		}
	}()

	// topic exchange queue
	// if err := client.CreateQueue("customers_queue", true, false); err != nil {
	// 	panic(fmt.Sprintf("RabbitMQ failed to create a queue: %v", err))
	// }

	// topic exchange binding
	// if err := client.CreateBinding("customers_queue", "customers.created.*", "customer_events"); err != nil {
	// 	panic(fmt.Sprintf("RabbitMQ failed to bind a queue to an exchange: %v", err))
	// }

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	// topic exchange
	// if err := client.Send(ctx, "customer_events", "customers.created.new_message", amqp091.Publishing{
	// 	ContentType: "text/plain",
	// 	DeliveryMode: amqp091.Persistent,
	// 	Body: []byte(`A new random message bla bla bla`),
	// }); err != nil {
	// 	panic(fmt.Sprintf("RabbitMQ failed to send a message to the exchange: %v", err))
	// }

	// fanout exchange
	// if err := client.Send(ctx, "customer_events_fan_out", "customers.created.new_message", amqp091.Publishing{
	// 	ContentType: "text/plain",
	// 	DeliveryMode: amqp091.Persistent,
	// 	Body: []byte(`A new random message bla bla bla`),
	// }); err != nil {
	// 	panic(fmt.Sprintf("RabbitMQ failed to send a message to the exchange: %v", err))
	// }

	var blocking chan string
	// direct exchange
	if err := client.Send(ctx, "customer_events_fan_out", "customers.created.test", amqp091.Publishing{
		ContentType: "text/plain",
		DeliveryMode: amqp091.Persistent,
		ReplyTo: q.Name,
		CorrelationId: fmt.Sprintf("customer_test"),
		Body: []byte(`A new random message bla bla bla`),
	}); err != nil {
		panic(fmt.Sprintf("RabbitMQ failed to send a message to the exchange: %v", err))
	}
	fmt.Println("Waiting for reply")
	<-blocking
}