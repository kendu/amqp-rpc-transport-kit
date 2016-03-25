package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/pborman/uuid"
	"github.com/streadway/amqp"
)

var Timeout time.Duration = time.Second * 5

type (
	Client struct {
		ch   *amqp.Channel
		conn *amqp.Connection

		cfg ClientConfig
	}

	Timeouts map[string]time.Duration

	ClientConfig struct {
		timeouts Timeouts
	}
)

func NewClient() *Client {
	return &Client{}
}

func (this *Client) SetTimeout(name string, timeout time.Duration) {
	if this.cfg.timeouts == nil {
		this.cfg.timeouts = Timeouts{}
	}
	this.cfg.timeouts[name] = timeout
}

func (this *Client) GetTimeout(name string) time.Duration {
	// Endpoint specific timeout
	if timeout, ok := this.cfg.timeouts[name]; ok {
		return timeout
	}

	// Catchall timeout
	if timeout, ok := this.cfg.timeouts["*"]; ok {
		return timeout
	}

	// Return default timeput
	return Timeout
}

func (this *Client) Open(uri string) (err error) {
	if this.conn, err = amqp.Dial(uri); err != nil {
		return
	}

	if this.ch, err = this.conn.Channel(); err != nil {
		return
	}

	return
}

func (this *Client) Close() {
	this.ch.Close()
	this.conn.Close()
}

/*
func (this *Client) getCall(name string) *Call {
	for _, ep := range this.calls {
		if ep.Name == name {
			return ep
		}
	}

	return nil
}
*/

func (this *Client) Call(name string, encoder Encoder, decoder Decoder) interface{} {
	timeout := this.GetTimeout(name)
	rsp := make(chan interface{}, 1)

	go func() { rsp <- this.call(name, encoder, decoder) }()

	select {
	case reachable := <-rsp:
		return reachable
	case <-time.After(timeout):
		// call timed out
		log.Printf("Call for %s timedout in %v", name, timeout)
		// if fallback != nil {
		// 	go fallback()
		// }
		return nil
	}
}

func (this *Client) call(name string, encoder Encoder, decoder Decoder) interface{} {
	q, err := this.ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when usused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := this.ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	payload := &bytes.Buffer{}

	failOnError(
		encoder(json.NewEncoder(payload)),
		"Failed to encode request",
	)

	corrId := uuid.New()

	err = this.ch.Publish(
		"",    // exchange
		name,  // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:   "application/json",
			ReplyTo:       q.Name,
			CorrelationId: corrId,
			Body:          payload.Bytes(),
		},
	)
	failOnError(err, "Failed to publish a message")

	for d := range msgs {
		if corrId == d.CorrelationId {
			failOnError(decoder(json.NewDecoder(bytes.NewBuffer(d.Body))), "Failed to decode response")
			return nil
		}
	}

	return nil
}

// @todo remove this
func failOnError(err error, msg string) {
	if err != nil {
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}
