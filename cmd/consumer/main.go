package main

import (
	"context"
	"golang.org/x/sync/errgroup"
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

	publishConn, err := internal.ConnectToRabbitMQ("testmest", "123", "localhost:5672", "customers")
	if err != nil {
		panic(fmt.Sprintf("RabbitMQ connection failed: %v", err))
	}

	defer conn.Close()

	publishClient, err := internal.NewRabbitMQClient(publishConn)
	if err != nil {
		panic(fmt.Sprintf("RabbitMQ failed to create a channel: %v", err))
	}

	defer func (publishClient internal.RabbitClient) {
		if err:= publishClient.Close(); err != nil {
			fmt.Printf("RabbitMQ failed to close the channel: %v\n", err)
		}		
	}(publishClient)
	
	//topic exchange
	// msgs, err := client.Read("customers_queue") 
	
	// if err != nil {
	// 	panic(fmt.Sprintf("RabbitMQ failed to create a channel: %v", err))
	// }

	// fanout queue
	q, err := client.CreateQueue("", true, false)

	if err != nil {
		panic(fmt.Sprintf("RabbitMQ failed to create a queue: %v", err))
	}

	// fanout binding
	// if err := client.CreateBinding(q.Name, "", "customer_events_fan_out"); err != nil {		
	// 	panic(fmt.Sprintf("RabbitMQ failed to create a binding: %v", err))
	// }

	// direct binding
	if err := client.CreateBinding(q.Name, "", "customer_events_fan_out"); err != nil {		
		panic(fmt.Sprintf("RabbitMQ failed to create a binding: %v", err))
	}

	msgs, err := client.Read(q.Name) 
	
	if err != nil {
		panic(fmt.Sprintf("RabbitMQ failed to create a channel: %v", err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15 * time.Second)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	// Can accept only 10 messages. The server will not send more than 10 messages
	if err := client.ApplyQos(10, 0, true); err != nil {
		panic(fmt.Sprintf("RabbitMQ failed to apply qos: %v", err))
	}
	
	// errgroup allow us to have concurent tasks
	g.SetLimit(10)
	
	var blocking chan string
	
	go func() {
		// waiting for messages
		for message := range msgs {	
			msg := message		
			g.Go(func() error {
				fmt.Println("New message: ", string(msg.Body))
				time.Sleep(10 * time.Second)				
				if err := msg.Ack(false); err != nil {
					fmt.Printf("RabbitMQ failed to ack a message: %v\n", err)
					return err
				}

				if err := publishClient.Send(ctx, "customer_events_direct", msg.ReplyTo, amqp091.Publishing{
					ContentType: "text/plain",
					DeliveryMode: amqp091.Persistent,
					CorrelationId: msg.CorrelationId,
					Body: []byte(`RPC complete`),
				}); err != nil {
					panic(fmt.Sprintf("RabbitMQ failed to send a message to the exchange: %v", err))
				}
				return nil
			})			
		}
	}()
	
	fmt.Println("Waiting for messages...")
	<-blocking
}