package session

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-xray-sdk-go/xray"
	http2 "github.com/cevixe/aws-sdk-go/aws/http"
	"net/http"
)

type sessionFactoryImpl struct {
	client   *http.Client
	warmer   http2.SslContextWarmer
	sessions map[string]*session.Session
}

func NewSessionFactory(client *http.Client) Factory {
	return &sessionFactoryImpl{
		client:   client,
		warmer:   http2.NewSslContextWarmer(client),
		sessions: make(map[string]*session.Session),
	}
}

func (f sessionFactoryImpl) GetSession(region string) *session.Session {
	if f.sessions[region] != nil {
		return f.sessions[region]
	}

	newSession := f.newSessionWithRegion(region)
	f.sessions[region] = newSession

	return newSession
}

func (f sessionFactoryImpl) newSessionWithRegion(region string) *session.Session {
	sess := session.Must(
		session.NewSessionWithOptions(
			session.Options{
				Config: aws.Config{
					Region:                  aws.String(region),
					S3ForcePathStyle:        aws.Bool(true),
					DisableParamValidation:  aws.Bool(true),
					DisableComputeChecksums: aws.Bool(true),
					HTTPClient:              f.client,
				},
				SharedConfigState: session.SharedConfigEnable,
			}))
	xray.AWSSession(sess)
	f.warmer.WarmUp(region, []string{"s3", "dynamodb", "sns"})
	return sess
}
