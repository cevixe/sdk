package object

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/jsii-runtime-go"
	"github.com/aws/smithy-go"
)

type clientImpl struct {
	bucket         *string
	presignClient  *s3.PresignClient
	standardClient *s3.Client
}

func (c *clientImpl) Exists(
	ctx context.Context, location string) (*bool, error) {

	_, err := c.standardClient.HeadObject(
		ctx,
		&s3.HeadObjectInput{
			Bucket: c.bucket,
			Key:    jsii.String(location),
		},
	)

	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			log.Printf("code: %s, message: %s, fault: %s", ae.ErrorCode(), ae.ErrorMessage(), ae.ErrorFault().String())
		}
		return nil, fmt.Errorf("cannot validate existence through headObject: %v", err)
	}

	return jsii.Bool(true), nil
}

func (c *clientImpl) Content(
	ctx context.Context, location string) (io.Reader, error) {

	output, err := c.standardClient.GetObject(
		ctx,
		&s3.GetObjectInput{
			Bucket: c.bucket,
			Key:    jsii.String(location),
		},
	)

	if err != nil {
		return nil, fmt.Errorf("cannot obtain content through getObject: %v", err)
	}

	return output.Body, nil
}

func (c *clientImpl) UploadURL(
	ctx context.Context, location string, duration time.Duration) (*string, error) {

	presignDuration := func(po *s3.PresignOptions) {
		po.Expires = duration
	}

	output, err := c.presignClient.PresignPutObject(
		ctx,
		&s3.PutObjectInput{
			Bucket: c.bucket,
			Key:    jsii.String(location),
		},
		presignDuration,
	)

	if err != nil {
		return nil, fmt.Errorf("cannot generate putObject presign url: %v", err)
	}

	return &output.URL, nil
}

func (c *clientImpl) DownloadURL(
	ctx context.Context, location string, duration time.Duration) (*string, error) {

	presignDuration := func(po *s3.PresignOptions) {
		po.Expires = duration
	}

	output, err := c.presignClient.PresignGetObject(
		ctx,
		&s3.GetObjectInput{
			Bucket: c.bucket,
			Key:    jsii.String(location),
		},
		presignDuration,
	)

	if err != nil {
		return nil, fmt.Errorf("cannot generate getObject presign url: %v", err)
	}

	return &output.URL, nil
}
