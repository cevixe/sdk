package runtime

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/events"
	cvxcontext "github.com/cevixe/sdk/context"
	"github.com/cevixe/sdk/handler"
	"github.com/cevixe/sdk/message"
	"github.com/cevixe/sdk/result"
	"github.com/pkg/errors"
)

func WrapHandler(hdl handler.Handler) interface{} {
	mode := os.Getenv("CVX_HANDLER_MODE")
	switch mode {
	case "advanced":
		return createSQSMessageHandler(hdl)
	case "standard":
		return createSQSMessageHandler(hdl)
	case "basic":
		return createSNSMessageHandler(hdl)
	default:
		log.Fatalln("handler execution mode not found")
		return nil
	}
}

func createSNSMessageHandler(hdl handler.Handler) interface{} {

	return func(ctx context.Context, input events.SNSEvent) error {

		if len(input.Records) != 1 {
			return errors.New("unsupported event stream configuration")
		}

		record := input.Records[0]
		msg, err := message.FromSNS(&record.SNS)
		if err != nil {
			return errors.Wrap(err, "cannot read sns message")
		}

		enrichedContext := loadExecutionContext(ctx, msg)
		res, err := hdl(enrichedContext, msg)
		if err != nil {
			return errors.Wrap(err, "unsuccessful execution of message handler")
		}

		return result.Write(enrichedContext, res)
	}
}

func createSQSMessageHandler(hdl handler.Handler) interface{} {

	return func(ctx context.Context, input events.SQSEvent) error {

		if len(input.Records) != 1 {
			return errors.New("unsupported event stream configuration")
		}

		record := input.Records[0]
		msg, err := message.FromSQS(record)
		if err != nil {
			return errors.Wrap(err, "cannot read sqs message")
		}

		enrichedContext := loadExecutionContext(ctx, msg)
		res, err := hdl(enrichedContext, msg)
		if err != nil {
			return errors.Wrap(err, "unsuccessful execution of message handler")
		}

		return result.Write(enrichedContext, res)
	}
}

func loadExecutionContext(ctx context.Context, msg message.Message) context.Context {

	return context.WithValue(ctx, cvxcontext.CevixeExecutionContextKey,
		&cvxcontext.ExecutionContext{
			Author:      msg.Author(),
			Trigger:     fmt.Sprintf("%s/%s", msg.Source(), msg.ID()),
			Transaction: msg.Transaction(),
		})
}
