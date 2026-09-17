package implementations

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func map_middleware_error(connection *amqp.Connection, err error) error {
	if err == nil {
		return nil
	}

	if connection == nil || connection.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}
	return m.ErrMessageMiddlewareClose
}

func close_middleware(connection *amqp.Connection) error {
	if connection == nil {
		return m.ErrMessageMiddlewareClose
	}

	if connection.IsClosed() {
		return nil
	}

	err := connection.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}
