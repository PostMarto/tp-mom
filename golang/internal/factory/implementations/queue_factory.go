package implementations

import (
	"fmt"
	"sync"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

var queue_amount_consumers uint64
var queue_lock_consumers sync.Mutex

type QueueFactory struct {
	id         string
	channel    *amqp.Channel
	connection *amqp.Connection
	queue      amqp.Queue
	exchange   string
	closing    bool
}

func NewQueueFactory(queueName string, connectionSettings m.ConnSettings) (QueueFactory, error) {
	factory := QueueFactory{}
	var err error
	factory.connection, err = amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port))
	if err != nil {
		defer factory.connection.Close()
		return QueueFactory{}, err
	}

	factory.channel, err = factory.connection.Channel()
	if err != nil {
		defer factory.channel.Close()
		return QueueFactory{}, err
	}

	factory.queue, err = factory.channel.QueueDeclare(
		queueName, // name
		true,      // durability
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,
	)

	if err != nil {
		defer factory.channel.Close()
		return QueueFactory{}, err
	}

	queue_lock_consumers.Lock()
	factory.id = fmt.Sprintf("%d", queue_amount_consumers)
	queue_amount_consumers++
	queue_lock_consumers.Unlock()
	factory.closing = false
	return factory, nil
}

// Comienza a escuchar a la cola/exchange e invoca a callbackFunc tras
// cada mensaje de datos o de control con el cuerpo del mensaje.
// callbackFunc tiene como parámetro:
// msg - El struct tal y como lo recibe el método Send.
// ack - Una función que hace ACK del mensaje recibido.
// nack - Una función que hace NACK del mensaje recibido.
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
func (factory *QueueFactory) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	messages, err := factory.channel.Consume(
		factory.queue.Name, // queue
		factory.id,         // consumer
		false,              // auto-ack
		false,              // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
	)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	for message := range messages {
		ack := func() {
			if err := message.Ack(false); err != nil {
				return
			}
		}

		nack := func() {
			if err := message.Nack(false, true); err != nil {
				return
			}
		}

		callbackFunc(m.Message{Body: string(message.Body)}, ack, nack)
	}

	return m.ErrMessageMiddlewareDisconnected
}

// Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
// no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
func (factory *QueueFactory) StopConsuming() error {
	if !factory.closing {
		factory.closing = true
		err := factory.channel.Cancel(factory.id, false)
		if err != nil {
			return m.ErrMessageMiddlewareDisconnected
		}
	}

	return nil
}

// Envía un mensaje a la cola o a los tópicos con el que se inicializó el exchange.
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
func (factory *QueueFactory) Send(msg m.Message) error {
	msg_to_publish := amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(msg.Body),
	}

	err := factory.channel.Publish("", factory.queue.Name, true, false, msg_to_publish)
	if err != nil {
		return map_middleware_error(factory.connection, err)
	}

	return nil
}

// Se desconecta de la cola o exchange al que estaba conectado.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareClose.
func (factory *QueueFactory) Close() error {
	factory.closing = true
	return close_middleware(factory.connection)
}
