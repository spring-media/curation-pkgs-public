package testcontainers

import (
	"bytes"
	"fmt"
	"net/http"
	"runtime"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

const openSearchVersion = "2.18.0"

func StartOpenSearch(tb testing.TB) *dockertest.Resource {
	tb.Helper()

	pool, err := dockertest.NewPool("")
	if err != nil {
		tb.Fatalf("Could not construct pool: %v", err)
	}
	pool.MaxWait = 3 * time.Minute

	env := []string{
		"discovery.type=single-node",
		"plugins.security.disabled=true",
		"DISABLE_INSTALL_DEMO_CONFIG=true",
	}

	// Only add UseSVE=0 for arm64 (see https://github.com/opensearch-project/OpenSearch/issues/16761)
	if runtime.GOARCH == "arm64" {
		env = append(env, "_JAVA_OPTIONS=-XX:UseSVE=0")
	}

	res, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "opensearchproject/opensearch",
		Tag:        openSearchVersion,
		Env:        env,
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.Memory = 4 * 1024 * 1024 * 1024
	})
	if err != nil {
		tb.Fatalf("Could not start resource: %v", err)
	}

	addCleanup(tb, func() { _ = pool.Purge(res) })

	hostPort := res.GetPort("9200/tcp")

	fmt.Printf("waiting for OpenSearch at http://localhost:%s\n", hostPort)
	err = pool.Retry(func() error {
		_, err := http.Get(fmt.Sprintf("http://localhost:%s/_cluster/health", hostPort))
		return err
	})
	if err != nil {
		var buf bytes.Buffer
		_ = pool.Client.Logs(docker.LogsOptions{
			Container:    res.Container.ID,
			OutputStream: &buf,
			ErrorStream:  &buf,
			Stderr:       true,
			Stdout:       true,
		})

		fmt.Println(buf.String())
		_ = pool.Purge(res)
		tb.Fatalf("could not connect to OpenSearch container: %s", err)
	}

	return res
}
