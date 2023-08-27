package internal

import (
	"context"
	"fmt"
	"time"

	//"context"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitClient struct {
	connection *amqp.Connection
	ch *amqp.Channel
}

func (rm *RabbitClient) Close() error {	
	return rm.ch.Close()	
}

// topic exchange queue
// func (rm *RabbitClient) CreateQueue(queueName string, durable, autodelete bool) error {
// 	_, err := rm.ch.QueueDeclare(queueName, durable, autodelete, false, false, nil)	
	
// 	return err	
// }

// fanout exchange
func (rm *RabbitClient) CreateQueue(queueName string, durable, autodelete bool) (amqp.Queue, error) {
	q, err := rm.ch.QueueDeclare(queueName, durable, autodelete, false, false, nil)	

	if err != nil {
		return amqp.Queue{}, err
	}
	
	return q, nil	
}

func (rm *RabbitClient) CreateBinding(name, binding, exchange string) error {	
	return rm.ch.QueueBind(name, binding, exchange, false, nil)		
}

func (rm *RabbitClient) Send(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error {
	//return rm.ch.PublishWithContext(ctx, exchange, routingKey, true, false, options)
	confirmation, err := rm.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, false, false, options)

	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	ok, err := confirmation.WaitContext(ctx)

	if err != nil {
		return fmt.Errorf("Error occured while waiting for the message to be confirmed by the exchange: %v\n", err)
	}

	if !ok {
		return fmt.Errorf("Failed to deliver the message to the exchange\n")
	}

	return nil
}

func (rm *RabbitClient) Read(queue string) (<-chan amqp.Delivery, error) {
	return rm.ch.Consume(queue, "", false, false, false, false, nil)	
}

// prefetch count - how many unack messages the server can send
// prefetch size - how many bytes the queue can have before we are allow to send more
// global flag - globaly applied rule or not
func (rm *RabbitClient) ApplyQos(count, size int, global bool) error {
	return rm.ch.Qos(count, size, global)
}

func NewRabbitMQClient(conn *amqp.Connection) (RabbitClient, error) {
	ch, err := conn.Channel()

	if err := ch.Confirm(false); err != nil {
		return RabbitClient{}, err
	}

	if err != nil {
		return RabbitClient{}, err
	}	

	return RabbitClient{
		connection: conn, 
		ch: ch,
	}, nil
}

func ConnectToRabbitMQ(username, password, host, vhost string) (*amqp.Connection, error) {	
	return amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", username, password, host, vhost))	
}