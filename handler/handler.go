package handler

import (
	"context"

	"github.com/cevixe/sdk/message"
	"github.com/cevixe/sdk/result"
)

type Handler func(ctx context.Context, msg message.Message) (result.Result, error)
