package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

type Endpoint struct {
	Name     string
	Endpoint Callback
}

type Server struct {
	ctx       context.Context
	ch        *amqp.Channel
	endpoints []*Endpoint
}

func NewServer(ep ...*Endpoint) *Server {
	return &Server{endpoints: ep}
}

func (this *Server) Add(ep ...*Endpoint) *Server {
	this.endpoints = append(this.endpoints, ep...)
	return this
}

func (this *Server) Listen(uri string) {
	conn, err := amqp.Dial(uri)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	this.ch, err = conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer this.ch.Close()

	err = this.ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)

	this.ctx = context.Background()
	this.setup()

	failOnError(err, "Failed to set QoS")
}

func (this *Server) setup() {
	fmt.Printf("Endpoints (%d) setup\n", len(this.endpoints))
	forever := make(chan bool)

	for _, ep := range this.endpoints {
		go func(ep *Endpoint) {
			fmt.Printf("Setting up %s endpoint\n", ep.Name)

			q, err := this.ch.QueueDeclare(
				ep.Name, // name
				false,   // durable
				false,   // delete when usused
				false,   // exclusive
				false,   // no-wait
				nil,     // arguments
			)
			failOnError(err, "Failed to declare a queue")

			msgs, err := this.ch.Consume(
				q.Name, // queue
				"",     // consumer
				false,  // auto-ack
				false,  // exclusive
				false,  // no-local
				false,  // no-wait
				nil,    // args
			)
			failOnError(err, "Failed to declare a queue")

			for d := range msgs {
				payload := &bytes.Buffer{}

				err := ep.Endpoint(
					json.NewDecoder(bytes.NewBuffer(d.Body)),
					json.NewEncoder(payload),
				)

				failOnError(err, "Failed to respond to a call")

				err = this.ch.Publish(
					"",        // exchange
					d.ReplyTo, // routing key
					false,     // mandatory
					false,     // immediate
					amqp.Publishing{
						ContentType: "application/json",
						// Make sure we are a match when we answer
						CorrelationId: d.CorrelationId,
						Body:          payload.Bytes(),
					},
				)

				failOnError(err, "Failed to publish a message")

				d.Ack(false)
			}
		}(ep)
	}

	log.Printf(" [*] Awaiting RPC requests")
	<-forever
}
