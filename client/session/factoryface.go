package session

import "github.com/aws/aws-sdk-go/aws/session"

type Factory interface {
	GetSession(region string) *session.Session
}
