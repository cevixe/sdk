package object

import (
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/jsii-runtime-go"
)

func NewClient(
	bucket string,
	presignClient *s3.PresignClient,
	standardClient *s3.Client,
) Client {

	return &clientImpl{
		bucket:         jsii.String(bucket),
		presignClient:  presignClient,
		standardClient: standardClient,
	}
}
