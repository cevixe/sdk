package object

import (
	"context"
	"io"
	"time"
)

type Client interface {
	Exists(ctx context.Context, location string) (*bool, error)
	Content(ctx context.Context, location string) (io.Reader, error)
	UploadURL(ctx context.Context, location string, duration time.Duration) (*string, error)
	DownloadURL(ctx context.Context, location string, duration time.Duration) (*string, error)
}
