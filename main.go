package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	url := "amqp://rabbitmaster:rabbitmaster@localhost:5672/"
	broker, err := NewBroker(url)
	if err != nil {
		log.Fatal(err)
	}

	broker.Consume()
}

type Broker struct {
	channel *amqp.Channel
	conn    *amqp.Connection
}

func NewBroker(url string) (*Broker, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &Broker{
		channel: ch,
		conn:    conn,
	}, nil
}

func (b *Broker) Close() error {
	err := b.conn.Close()
	if err != nil {
		return err
	}
	err = b.channel.Close()
	if err != nil {
		return err
	}
	return nil
}

type Transaction struct {
	ID             int64
	SourceWalletID int32
	Amount         float64
	Currency       string
	TargetWalletID int32
	Status         string
}

func (b *Broker) Consume() {
	messages, err := b.channel.Consume(
		"transaction.created", 
		"",                    
		true,                  
		false,                 
		false,                 
		false,                 
		nil,                   
	)
	if err != nil {
		log.Fatalf("failed to register a consumer. Error: %s", err)
	}

	var forever chan struct{}
	go func() {
		for message := range messages {
			body := Transaction{}
			err := json.Unmarshal(message.Body, &body)
			if err != nil {
				log.Fatal("Failed message")
			}

			dataAccount := UpdateAccountAmountRequest{Amount: body.Amount, Currency: body.Currency}
			payloadAccount, err := json.Marshal(dataAccount)
			if err != nil {
				log.Fatal("Failed message")
			}

			client := &http.Client{}

			req, err := http.NewRequest("PATCH", fmt.Sprintf("http://localhost:8080/api/v1/account/%v", body.SourceWalletID), bytes.NewBuffer(payloadAccount))
			if err != nil {
				log.Fatal("Failed message")
			}
			resp, err := client.Do(req)
			if err != nil {
				log.Fatal("Failed message")
			}
			if resp.StatusCode != 200 {
				log.Println(resp)
				log.Println("Failed update account")

			}

			dataTransaction := UpdateTransactionsRequest{Status: "success"}
			payloadTransaction, err := json.Marshal(dataTransaction)
			if err != nil {
				log.Fatal("Failed message")
			}
			reqT, err := http.NewRequest("PATCH", fmt.Sprintf("http://localhost:8080/api/v1/transaction/%v", body.ID), bytes.NewBuffer(payloadTransaction))
			if err != nil {
				log.Fatal("Failed message")
			}
			respT, err := client.Do(reqT)
			if err != nil {
				log.Fatal("Failed message")
			}
			if respT.StatusCode != 200 {
				log.Println("Failed update account")

			}
		}
	}()
	<-forever
}

type UpdateAccountAmountRequest struct {
	ID       int64   `json:"id"`
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency,omitempty"`
}

type UpdateTransactionsRequest struct {
	ID     int64  `json:"id"`
	Status string `json:"status"`
}
