package runtime

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/cevixe/sdk/handler"
)

func Start(hdl handler.Handler) {
	ctx := NewContext()
	lmb := WrapHandler(hdl)
	lambda.StartWithOptions(lmb, lambda.WithContext(ctx))
}
