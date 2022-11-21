package http

type SslContextWarmer interface {
	WarmUp(region string, services []string)
}
