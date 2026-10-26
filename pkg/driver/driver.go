/*
 Copyright 2022 Juicedata Inc

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
	"errors"
	"net"
	"sync"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"

	"github.com/juicedata/juicefs-csi-driver/pkg/config"
	"github.com/juicedata/juicefs-csi-driver/pkg/k8sclient"
	"github.com/juicedata/juicefs-csi-driver/pkg/util"
)

var (
	driverLog = klog.NewKlogr().WithName("driver")
)

// Driver struct
type Driver struct {
	csi.UnimplementedIdentityServer

	*controllerService
	nodeService
	provisionerService

	mu       sync.Mutex // guards srv and stopped during startup and shutdown
	srv      *grpc.Server
	stopped  bool
	endpoint string
}

// NewDriver creates a new driver
func NewDriver(endpoint string, nodeID string,
	leaderElection bool,
	leaderElectionNamespace string,
	leaderElectionLeaseDuration time.Duration, reg prometheus.Registerer) (*Driver, error) {
	driverLog.Info("get version info", "driver", config.DriverName, "verison", driverVersion, "commit", gitCommit, "date", buildDate)

	var k8sClient *k8sclient.K8sClient
	if !config.ByProcess {
		var err error
		k8sClient, err = k8sclient.NewClient()
		if err != nil {
			driverLog.Error(err, "Can't get k8s client")
			return nil, err
		}
	}
	cs, err := newControllerService(k8sClient)
	if err != nil {
		return nil, err
	}

	ns, err := newNodeService(nodeID, k8sClient, reg)
	if err != nil {
		return nil, err
	}

	ps, err := newProvisionerService(k8sClient, leaderElection, leaderElectionNamespace, leaderElectionLeaseDuration, reg)
	if err != nil {
		return nil, err
	}

	return &Driver{
		controllerService:  cs,
		nodeService:        *ns,
		provisionerService: ps,
		endpoint:           endpoint,
	}, nil
}

// Run runs the server
func (d *Driver) Run() error {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return nil
	}
	scheme, addr, err := util.ParseEndpoint(d.endpoint)
	if err != nil {
		d.mu.Unlock()
		return err
	}

	listener, err := net.Listen(scheme, addr)
	if err != nil {
		d.mu.Unlock()
		return err
	}

	logErr := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			driverLog.Error(err, "GRPC error")
		}
		return resp, err
	}
	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(logErr),
	}
	srv := grpc.NewServer(opts...)

	csi.RegisterIdentityServer(srv, d)
	csi.RegisterControllerServer(srv, d)
	csi.RegisterNodeServer(srv, d)
	d.srv = srv
	if config.Provisioner {
		go d.provisionerService.Run(context.Background())
	}
	d.mu.Unlock()

	driverLog.Info("Listening for connection on address", "address", listener.Addr())
	err = srv.Serve(listener)
	// Stop may run after initialization but before Serve starts accepting requests.
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

// Stop stops server
func (d *Driver) Stop() {
	d.mu.Lock()
	d.stopped = true
	srv := d.srv
	d.mu.Unlock()
	if srv != nil {
		srv.Stop()
	}
	driverLog.Info("Stopped server")
}
