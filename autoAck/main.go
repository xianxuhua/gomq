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
	for i := 0; i < 3; i++ {
		body := "Hello, RabbitMQ!"
		err = ch.Publish(
			"fanout_exchange", // exchange
			"",                // routing key
			false,
			// mandatory,false表示如果消息无法路由到任何队列，RabbitMQ不会返回消息给发送者
			// 即使将mandatory设置为true，RabbitMQ也仅在以下情况下返回消息
			// 消息无法路由到任何队列，因为没有匹配的绑定队列
			// 队列存在但当前不可达
			// 对于其他情况，例如消息已成功路由到队列但消费者无法处理它，RabbitMQ不会返回消息给发送者
			// 这种情况下，需要使用其他机制（如死信队列、消费者确认等）来处理失败的消息
			false, // immediate
			amqp.Publishing{
				Body: []byte(body),
			})
		failOnError(err, "Failed to publish a message")
	}

	// 确认机制只确保消息被RabbitMQ服务器接收，并不能确保消息被消费者成功处理
	// 如果需要确保消息被消费者成功处理，需要死信队列
	confirmChan := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	for i := 0; i < 3; i++ {
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
		true,  // auto ack
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
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
