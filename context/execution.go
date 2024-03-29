package context

import "context"

type ExecutionContext struct {
	Author      string
	Trigger     string
	Transaction string
}

func GetExecutionContenxt(ctx context.Context) *ExecutionContext {
	return ctx.Value(CevixeExecutionContextKey).(*ExecutionContext)
}
