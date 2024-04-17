package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/host1")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	// Qos的前提的不使用自动确认
	// 设置QoS参数：预取值为1
	err = ch.Qos(
		3, // 同一最多有3个消息被一个消费者处理，
		// 增加消费者时，同时处理的数量为prefetchCount * consumerCount,
		// 若未开启Qos，一堆消息直接发到刚开始建立的消费者身上，横行扩展没有用
		// rabbitmq仅实现了这一个参数
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	// 异步确认
	err = ch.Confirm(false)
	failOnError(err, "Failed to set confirm mode")

	// 声明一个fanout交换器
	err = ch.ExchangeDeclare(
		"fanout_exchange", // exchange name
		"fanout",          // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	// 创建并绑定多个队列到交换器
	for i := 0; i < 3; i++ {
		q, err := ch.QueueDeclare(
			"",    // name
			false, // durable
			false, // delete when unused
			true,  // exclusive。创建它的连接断开时，队列会被自动删除
			false, // no-wait
			nil,   // arguments
		)
		failOnError(err, "Failed to declare a queue")

		err = ch.QueueBind(
			q.Name,            // queue name
			"",                // routing key
			"fanout_exchange", // exchange name
			false,
			nil)
		failOnError(err, "Failed to bind a queue")

		go consume(ch, q.Name)
	}

	// 发布消息到交换器，它将广播到所有队列
	for i := 0; i < 50; i++ {
		body := "Hello, RabbitMQ!"
		err = ch.Publish(
			"fanout_exchange", // exchange
			"",                // routing key
			false,             // mandatory
			false,             // immediate
			amqp.Publishing{
				Body:       []byte(body),
				Expiration: "5000", // 设置消息过期时间为5s,过期的消息被丢弃
			})
		failOnError(err, "Failed to publish a message")
	}

	// 确认机制只确保消息被RabbitMQ服务器接收，并不能确保消息被消费者成功处理
	// 如果需要确保消息被消费者成功处理，需要死信队列
	confirmChan := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	for i := 0; i < 50; i++ {
		select {
		case conf := <-confirmChan:
			if conf.Ack {
				log.Printf("Message confirmed")
			} else {
				log.Printf("Message nacked")
			}
		case <-time.After(time.Second * 5):
			log.Printf("Publish confirmation timeout")
		}
	}

	// 等待信号，优雅地关闭连接
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
}

func consume(ch *amqp.Channel, qName string) {
	msgs, err := ch.Consume(
		qName, // queue
		"",    // consumer
		false, // auto ack，手动确认需要为false
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("%s Received a message: %s", qName, d.Body)

			time.Sleep(time.Second) // 模拟执行时间

			// 消费者确认，false表示不希望批量确认消息
			err = d.Ack(false)
			failOnError(err, "Failed to deliver a message")
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
