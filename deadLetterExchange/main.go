package main

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

func main() {
	initConsumer1()
}

const (
	NORMAL_EXCHANGE   = "normal_exchange"
	DEAD_EXCHANGE     = "dead_exchange"
	NORMAL_QUEUE      = "normal_queue"
	DEAD_QUEUE        = "dead_queue"
	NORMAL_ROUTINGKEY = "normal"
	DEAD_ROUTINGKEY   = "dead"
	EXCHANGE_TYPE     = "direct"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func initConsumer1() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/host1")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	// 1. 声明 normal_exchange
	err = ch.ExchangeDeclare(
		NORMAL_EXCHANGE,
		EXCHANGE_TYPE,
		true,
		false,
		false,
		false,
		nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	// 2. 声明 dead_exchange
	err = ch.ExchangeDeclare(
		DEAD_EXCHANGE,
		EXCHANGE_TYPE,
		true,
		false,
		false,
		false,
		nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	// 3. 声明 normal_queue
	args := amqp.Table{
		"x-dead-letter-exchange":    DEAD_EXCHANGE,   //死信队列交换机
		"x-dead-letter-routing-key": DEAD_ROUTINGKEY, //死信队列routing key
		//"x-max-length":              2,               //测试二：队列最大长度
	}
	normal_q, err := ch.QueueDeclare(
		NORMAL_QUEUE,
		false,
		false,
		false,
		false,
		args,
	)

	// 4. 声明 dead_queue
	dead_q, err := ch.QueueDeclare(
		DEAD_QUEUE,
		false,
		false,
		false,
		false,
		nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	//5. 队列绑定
	err = ch.QueueBind(
		normal_q.Name,
		NORMAL_ROUTINGKEY,
		NORMAL_EXCHANGE,
		false,
		nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = ch.QueueBind(
		dead_q.Name,
		DEAD_ROUTINGKEY,
		DEAD_EXCHANGE,
		false,
		nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	go publishMessage(ch, "abandon")

	//time.Sleep(time.Second * 6) # 消费超时消息进入死信队列
	//6. 接受normal_queue中的消息
	msgs, err := ch.Consume(
		normal_q.Name,
		"c1",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	forever := make(chan interface{})
	go func() {
		for msg := range msgs {
			if string(msg.Body) == "abandon" {
				fmt.Println("c1 abandon normal msg -->", string(msg.Body))
				msg.Nack(false, false) //拒绝消息，消息进入死信队列
			} else {
				fmt.Println("c1 receive normal msg -->", string(msg.Body))
				msg.Ack(false) //确认消息
			}

		}
	}()

	<-forever
}

func publishMessage(mqCh *amqp.Channel, msg string) {
	if mqCh == nil {
		fmt.Println("channel is nil")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mqCh.PublishWithContext(
		ctx,
		NORMAL_EXCHANGE,
		NORMAL_ROUTINGKEY,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg),
			Expiration:  "5000", // 测试一： 设置过期时间为5s
		},
	)
	fmt.Println("publish msg--->", msg)
}
