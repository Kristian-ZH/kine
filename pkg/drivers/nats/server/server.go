//go:build nats
// +build nats

package server

import (
	"fmt"
	"net/url"
	"os"

	"github.com/nats-io/nats-server/v2/server"
)

const (
	Embedded = true
)

func New(c *Config) (Server, error) {
	opts := &server.Options{}

	if c.ConfigFile != "" {
		// Parse the server config file as options.
		var err error
		opts, err = server.ProcessConfigFile(c.ConfigFile)
		if err != nil {
			return nil, fmt.Errorf("failed to process NATS server config file: %w", err)
		}
	}

	// Note, if don't listen is set, host and port will be ignored.
	opts.DontListen = c.DontListen

	// Only override host and port if set explicitly.
	if c.Host != "" {
		opts.Host = c.Host
	}
	if c.Port != 0 {
		opts.Port = c.Port
	}

	// TODO: Other defaults for embedded config?
	// Explicitly set JetStream to true since we need the KV store.
	opts.JetStream = true

	clusterPort := os.Getenv("HOST_IP")

	opts.Cluster = server.ClusterOpts{
		Name: "test-cluster",
		Host: "0.0.0.0",
		Port: 4248,
	}

	if clusterPort != "4248" {
		opts.Routes = []*url.URL{
			{
				Host:   "0.0.0.0:4248",
				Scheme: "nats",
			},
		}
	}

	srv, err := server.NewServer(opts)
	if c.StdoutLogging {
		srv.ConfigureLogger()
	}

	return srv, err
}
