package config

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/cevixe/sdk/client/http"
)

func NewConfig(ctx context.Context) aws.Config {

	region := os.Getenv("AWS_REGION")
	httpClient := http.NewDefaultClient()
	http.WarmUp(httpClient, region, []string{"dynamodb", "sns", "s3"})
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithDefaultRegion(region),
		config.WithHTTPClient(httpClient),
	)

	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	return cfg
}
