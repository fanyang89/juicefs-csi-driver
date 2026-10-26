/*
 Copyright 2026 Juicedata Inc

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package driver

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func runTestDriver(d *Driver) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- d.Run()
	}()
	return done
}

func waitTestDriver(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() returned an error after Stop(): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after Stop()")
	}
}

func TestDriverStopBeforeRun(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "csi.sock")
	// A stopped driver must not even remove an existing endpoint during startup.
	if err := os.WriteFile(socket, []byte("untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	d := &Driver{endpoint: "unix://" + socket}
	d.Stop()
	d.Stop()
	waitTestDriver(t, runTestDriver(d))
	contents, err := os.ReadFile(socket)
	if err != nil || string(contents) != "untouched" {
		t.Fatalf("Run() modified the endpoint after Stop(): contents=%q, err=%v", contents, err)
	}
}

func TestDriverRunStop(t *testing.T) {
	d := &Driver{
		controllerService: &controllerService{},
		endpoint:          "unix://" + filepath.Join(t.TempDir(), "csi.sock"),
	}
	done := runTestDriver(d)
	t.Cleanup(d.Stop)
	conn, err := grpc.NewClient(d.endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := csi.NewIdentityClient(conn).Probe(ctx, &csi.ProbeRequest{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("driver did not serve CSI requests: %v", err)
	}
	var stoppers sync.WaitGroup
	for range 8 {
		stoppers.Go(d.Stop)
	}
	stoppers.Wait()
	d.Stop()
	waitTestDriver(t, done)
	waitTestDriver(t, runTestDriver(d))
}

func TestDriverConcurrentRunStop(t *testing.T) {
	endpoint := "unix://" + filepath.Join(t.TempDir(), "csi.sock")
	for range 100 {
		d := &Driver{controllerService: &controllerService{}, endpoint: endpoint}
		t.Cleanup(d.Stop)
		start := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			<-start
			done <- d.Run()
		}()
		var stoppers sync.WaitGroup
		for range 8 {
			stoppers.Go(func() {
				<-start
				d.Stop()
			})
		}
		close(start)
		stoppers.Wait()
		waitTestDriver(t, done)
		waitTestDriver(t, runTestDriver(d))
	}
}

func TestDriverStopAfterRunError(t *testing.T) {
	for _, endpoint := range []string{
		"unsupported://endpoint",
		"unix://" + filepath.Join(t.TempDir(), "missing", "csi.sock"),
	} {
		t.Run(endpoint, func(t *testing.T) {
			d := &Driver{endpoint: endpoint}
			if err := d.Run(); err == nil {
				t.Fatal("Run() succeeded with an invalid endpoint")
			}
			d.Stop()
			d.Stop()
			waitTestDriver(t, runTestDriver(d))
		})
	}
}
