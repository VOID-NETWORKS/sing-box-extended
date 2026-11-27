package mieru

// The mieru server, after apis/server/server.go of github.com/enfein/mieru
// (Copyright (C) 2025 mieru authors, GPL-3.0-or-later), with one addition:
// its users can be replaced while it runs. The library's own server hides
// the multiplexer that can do this.

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	mierucommon "github.com/enfein/mieru/v3/apis/common"
	mierumodel "github.com/enfein/mieru/v3/apis/model"
	mieruserver "github.com/enfein/mieru/v3/apis/server"
	mierutp "github.com/enfein/mieru/v3/apis/trafficpattern"
	"github.com/enfein/mieru/v3/pkg/appctl/appctlcommon"
	mierupb "github.com/enfein/mieru/v3/pkg/appctl/appctlpb"
	mieruutil "github.com/enfein/mieru/v3/pkg/common"
	mierulog "github.com/enfein/mieru/v3/pkg/log"
	"github.com/enfein/mieru/v3/pkg/protocol"
)

var silenceMieruLog sync.Once

type server struct {
	mu      sync.Mutex
	running atomic.Bool
	config  *mieruserver.ServerConfig
	mux     *protocol.Mux
}

func newServer(config *mieruserver.ServerConfig) (*server, error) {
	silenceMieruLog.Do(func() { mierulog.SetFormatter(&mierulog.NilFormatter{}) })
	if config == nil || config.Config == nil {
		return nil, fmt.Errorf("server config is nil")
	}
	if len(config.Config.GetPortBindings()) == 0 {
		return nil, fmt.Errorf("server port bindings are not set")
	}
	if _, err := appctlcommon.FlatPortBindings(config.Config.GetPortBindings()); err != nil {
		return nil, err
	}
	if err := validateUsers(config.Config.GetUsers()); err != nil {
		return nil, err
	}
	if err := mierutp.Validate(config.Config.TrafficPattern); err != nil {
		return nil, fmt.Errorf("invalid traffic pattern: %w", err)
	}
	return &server{config: config}, nil
}

// validateUsers checks a list of users the way mieru does. The multiplexer
// refuses to start without one; once it runs, the list may become empty.
func validateUsers(users []*mierupb.User) error {
	for _, user := range users {
		if err := appctlcommon.ValidateServerConfigSingleUser(user); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.config.Config.GetUsers()) == 0 {
		return fmt.Errorf("users is empty")
	}
	s.mux = protocol.NewMux(false)
	if s.config.StreamListenerFactory != nil {
		s.mux.SetStreamListenerFactory(s.config.StreamListenerFactory)
	}
	if s.config.PacketListenerFactory != nil {
		s.mux.SetPacketListenerFactory(s.config.PacketListenerFactory)
	}
	trafficPattern, err := mierutp.NewConfig(s.config.Config.TrafficPattern)
	if err != nil {
		return err
	}
	s.mux.SetTrafficPattern(trafficPattern).
		SetServerUsers(appctlcommon.UserListToMap(s.config.Config.GetUsers())).
		SetServerUserHintIsMandatory(s.config.Config.GetAdvancedSettings().GetUserHintIsMandatory())
	mtu := mieruutil.DefaultMTU
	if s.config.Config.GetMtu() != 0 {
		mtu = int(s.config.Config.GetMtu())
	}
	endpoints, err := appctlcommon.PortBindingsToUnderlayProperties(s.config.Config.GetPortBindings(), mtu)
	if err != nil {
		return err
	}
	s.mux.SetEndpoints(endpoints)
	if err := s.mux.Start(); err != nil {
		return err
	}
	s.running.Store(true)
	return nil
}

// SetUsers replaces the users. Established connections keep working; new
// ones are checked against the new list.
func (s *server) SetUsers(users []*mierupb.User) error {
	if err := validateUsers(users); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config.Config.Users = users
	if s.mux != nil {
		s.mux.SetServerUsers(appctlcommon.UserListToMap(users))
	}
	return nil
}

func (s *server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running.Store(false)
	if s.mux != nil {
		s.mux.Close()
	}
	return nil
}

func (s *server) IsRunning() bool { return s.running.Load() }

func (s *server) Accept() (net.Conn, *mierumodel.Request, error) {
	conn, err := s.mux.Accept()
	if err != nil {
		return nil, nil, err
	}
	if _, ok := conn.(mierucommon.UserContext); !ok {
		return nil, nil, fmt.Errorf("internal error: connection doesn't implement UserContext interface")
	}
	mieruutil.SetReadTimeout(conn, 10*time.Second)
	defer mieruutil.SetReadTimeout(conn, 0)
	req := &mierumodel.Request{}
	if err := req.ReadFromSocks5(conn); err != nil {
		return nil, nil, err
	}
	return conn, req, nil
}
