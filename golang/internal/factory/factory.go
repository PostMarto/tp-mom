package factory

import (
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/implementations"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	factory, err := implementations.NewQueueFactory(queueName, connectionSettings)
	if err != nil {
		return nil, err
	}
	return &factory, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	factory, err := implementations.NewExchangeFactory(exchange, keys, connectionSettings)
	if err != nil {
		return nil, err
	}
	return &factory, nil
}
