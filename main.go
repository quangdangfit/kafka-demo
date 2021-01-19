package main

import (
	"context"

	"github.com/quangdangfit/kafka-demo/consumer"
	"github.com/quangdangfit/kafka-demo/producer"
)

func main() {
	// create a new context
	ctx := context.Background()
	// produce messages in a new go routine, since
	// both the produce and consume functions are
	// blocking
	go producer.Produce(ctx)
	consumer.Consume(ctx)
}
