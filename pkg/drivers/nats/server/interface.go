package server

import (
	"net"
	"time"
)

type Server interface {
	Start()
	Shutdown()
	ClientURL() string
	ReadyForConnections(wait time.Duration) bool
	JetStreamIsCurrent() bool
	JetStreamIsStreamCurrent(account, stream string) bool
	InProcessConn() (net.Conn, error)
}

type Config struct {
	Host          string
	Port          int
	ConfigFile    string
	DontListen    bool
	StdoutLogging bool
	DataDir       string
}
