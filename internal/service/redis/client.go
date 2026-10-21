package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	rediscli "github.com/redis/go-redis/v9"
)

const (
	redisRoleMaster = "role:master"
)

type ConnectionInfo struct {
	// Host is the IP address or hostname for connection
	Host string
	// Port is the port for redis or sentinel
	Port     string
	Password string
	// TLSConfig configuration, nil means TLS is disabled
	TLSConfig *tls.Config
}

// ClusterStatus cluster status information, including the number of assigned slots
type ClusterStatus struct {
	SlotsAssigned int // Total number of assigned slots
}

// GetAddress returns the connection address
func (c *ConnectionInfo) GetAddress() string {
	return net.JoinHostPort(c.Host, c.Port)
}

type Client interface {
	Connect(info *ConnectionInfo) Service
}

func (c *client) Connect(info *ConnectionInfo) Service {
	service := &service{}
	service.connectionInfo = info
	return service
}

type client struct{}

func NewClient() Client {
	return &client{}
}

type Service interface {
	IsMaster(ctx context.Context) (bool, error)
	GetAttachedReplicaCount(ctx context.Context) (int, error)
	GetKeyCount(ctx context.Context) (int64, error)
	SentinelMonitor(ctx context.Context, master *ConnectionInfo, masterGroupName, quorum string) error
	SentinelSet(ctx context.Context, masterGroupName, key, value string) error
	SentinelReset(ctx context.Context, masterGroupName string) error
	GetInfoSentinel(ctx context.Context) (*InfoSentinelResult, error)
	// SentinelGetMasterAddrByName returns the address this Sentinel currently
	// considers the master of masterGroupName. ok is false when the Sentinel
	// does not monitor that group.
	SentinelGetMasterAddrByName(ctx context.Context, masterGroupName string) (host, port string, ok bool, err error)
	GetClusterInfo(ctx context.Context) (*ClusterStatus, error)
}

type InfoSentinelResult struct {
	Masters []SentinelMasterInfo
}

type SentinelMasterInfo struct {
	Name      string
	Status    string
	Address   string
	Slaves    int
	Sentinels int
}

func (r *InfoSentinelResult) Master(name string) (SentinelMasterInfo, bool) {
	if r == nil {
		return SentinelMasterInfo{}, false
	}
	for _, master := range r.Masters {
		if master.Name == name {
			return master, true
		}
	}
	return SentinelMasterInfo{}, false
}

func (m SentinelMasterInfo) HasStaleEntries(expectedSlaves, expectedSentinels int) bool {
	return m.Slaves > expectedSlaves || m.Sentinels > expectedSentinels
}

type service struct {
	connectionInfo *ConnectionInfo
	redisClient    *rediscli.Client
}

func NewService() Service {
	return &service{}
}

func (s *service) createClient() *rediscli.Client {
	if s.redisClient != nil {
		return s.redisClient
	}
	if s.connectionInfo == nil {
		return nil
	}
	opts := &rediscli.Options{
		Addr:     s.connectionInfo.GetAddress(),
		Password: s.connectionInfo.Password,
		DB:       0,
		// Honour caller deadlines; go-redis otherwise applies only its own
		// 5 s dial and 3 s read timeouts.
		ContextTimeoutEnabled: true,
	}
	if s.connectionInfo.TLSConfig != nil {
		opts.TLSConfig = s.connectionInfo.TLSConfig
	}
	return rediscli.NewClient(opts)
}

func (c *service) GetInfoSentinel(ctx context.Context) (*InfoSentinelResult, error) {
	client := c.createClient()
	if client == nil {
		return nil, nil
	}
	defer client.Close()

	result, err := client.Info(ctx, "sentinel").Result()
	if err != nil {
		return nil, err
	}

	info := &InfoSentinelResult{}

	// Parse sentinel section
	// Expected format: master0:name=myMaster,status=ok,address=10.233.93.209:6379,slaves=2,sentinels=1
	for _, line := range strings.Split(result, "\r\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "master") && strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) != 2 {
				continue
			}

			masterData := make(map[string]string)
			for _, pair := range strings.Split(parts[1], ",") {
				kv := strings.SplitN(pair, "=", 2)
				if len(kv) == 2 {
					masterData[kv[0]] = kv[1]
				}
			}

			master := SentinelMasterInfo{
				Name:    masterData["name"],
				Status:  masterData["status"],
				Address: masterData["address"],
			}
			if slavesStr, ok := masterData["slaves"]; ok {
				if slaves, err := strconv.Atoi(slavesStr); err == nil {
					master.Slaves = slaves
				}
			}
			if sentinelsStr, ok := masterData["sentinels"]; ok {
				if sentinels, err := strconv.Atoi(sentinelsStr); err == nil {
					master.Sentinels = sentinels
				}
			}

			info.Masters = append(info.Masters, master)
		}
	}

	return info, nil
}

func (c *service) SentinelGetMasterAddrByName(ctx context.Context, masterGroupName string) (string, string, bool, error) {
	client := c.createClient()
	if client == nil {
		return "", "", false, nil
	}
	defer client.Close()

	addr, err := client.Do(ctx, "SENTINEL", "GET-MASTER-ADDR-BY-NAME", masterGroupName).StringSlice()
	if errors.Is(err, rediscli.Nil) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, err
	}
	if len(addr) != 2 {
		return "", "", false, fmt.Errorf("unexpected SENTINEL GET-MASTER-ADDR-BY-NAME reply %q", addr)
	}
	return addr[0], addr[1], true, nil
}

func (c *service) SentinelSet(ctx context.Context, masterGroupName, key, value string) error {
	client := c.createClient()
	if client == nil {
		return nil
	}
	defer client.Close()

	cmd := rediscli.NewStringCmd(ctx, "SENTINEL", "SET", masterGroupName, key, value)
	err := client.Process(ctx, cmd)
	if err != nil {
		return err
	}
	if err = cmd.Err(); err != nil {
		return err
	}
	return nil
}

func (c *service) SentinelReset(ctx context.Context, masterGroupName string) error {
	client := c.createClient()
	if client == nil {
		return nil
	}
	defer client.Close()

	cmd := rediscli.NewStringCmd(ctx, "SENTINEL", "RESET", masterGroupName)
	err := client.Process(ctx, cmd)
	if err != nil {
		return err
	}
	if err = cmd.Err(); err != nil {
		return err
	}
	return nil
}

func (c *service) SentinelMonitor(ctx context.Context, master *ConnectionInfo, masterGroupName, quorum string) error {
	var (
		cmd *rediscli.BoolCmd
		err error
	)

	client := c.createClient()
	if client == nil {
		return nil
	}
	defer client.Close()

	masterExists := false
	masterCheckCmd := rediscli.NewMapStringStringCmd(ctx, "SENTINEL", "MASTER", masterGroupName)
	if err = client.Process(ctx, masterCheckCmd); err == nil {
		if err = masterCheckCmd.Err(); err == nil {
			masterExists = true
			result := masterCheckCmd.Val()
			if result["ip"] == master.Host && result["port"] == master.Port {
				return nil
			}
		}
	}

	// Legacy Sentinel entrypoints may ignore hostname environment settings.
	// Check the live resolver before removing an existing monitor: otherwise
	// MONITOR rejects the hostname after REMOVE has already erased the group.
	if net.ParseIP(master.Host) == nil {
		// The typed map command reads RESP2 key/value arrays and RESP3 maps.
		configCmd := rediscli.NewMapStringStringCmd(ctx, "SENTINEL", "CONFIG", "GET", "resolve-hostnames")
		if configErr := client.Process(ctx, configCmd); configErr != nil {
			return fmt.Errorf("verify Sentinel resolve-hostnames before changing monitor: %w", configErr)
		}
		settings, configErr := configCmd.Result()
		if configErr != nil {
			return fmt.Errorf("verify Sentinel resolve-hostnames before changing monitor: %w", configErr)
		}
		if settings["resolve-hostnames"] != "yes" {
			return fmt.Errorf("sentinel resolve-hostnames must be enabled before changing monitor to a hostname; existing monitor preserved")
		}
	}

	// Only remove if master was already being monitored (with wrong host/port).
	// On first-time setup, SENTINEL REMOVE would fail with "ERR No such master
	// with that name" and prevent SENTINEL MONITOR from ever being called.
	if masterExists {
		cmd = rediscli.NewBoolCmd(ctx, "SENTINEL", "REMOVE", masterGroupName)
		err = client.Process(ctx, cmd)
		if err != nil {
			return err
		}
		if err = cmd.Err(); err != nil {
			return err
		}
	}

	cmd = rediscli.NewBoolCmd(ctx, "SENTINEL", "MONITOR", masterGroupName, master.Host, master.Port, quorum)
	err = client.Process(ctx, cmd)
	if err != nil {
		return err
	}
	if err = cmd.Err(); err != nil {
		return err
	}

	if master.Password != "" {
		cmd = rediscli.NewBoolCmd(ctx, "SENTINEL", "SET", masterGroupName, "auth-pass", master.Password)
		err = client.Process(ctx, cmd)
		if err != nil {
			return err
		}
		if err = cmd.Err(); err != nil {
			return err
		}
	}

	return nil
}

func (c *service) IsMaster(ctx context.Context) (bool, error) {
	client := c.createClient()
	if client == nil {
		return false, nil
	}
	defer client.Close()

	result, err := client.Info(ctx, "replication").Result()
	if err != nil {
		return false, err
	}
	return strings.Contains(result, redisRoleMaster), nil
}

func (c *service) GetAttachedReplicaCount(ctx context.Context) (int, error) {
	client := c.createClient()
	if client == nil {
		return 0, nil
	}
	defer client.Close()

	result, err := client.Info(ctx, "replication").Result()
	if err != nil {
		return 0, err
	}

	var count int
	for _, line := range strings.Split(result, "\r\n") {
		if value, found := strings.CutPrefix(line, "connected_slaves:"); found {
			count, err = strconv.Atoi(value)
			if err != nil {
				return 0, err
			}
			return count, nil
		}
	}
	return count, nil
}

func (c *service) GetKeyCount(ctx context.Context) (int64, error) {
	client := c.createClient()
	if client == nil {
		return 0, errors.New("redis connection info is not set")
	}
	defer client.Close()

	result, err := client.Info(ctx, "keyspace").Result()
	if err != nil {
		return 0, err
	}
	return countKeyspaceKeys(result)
}

func countKeyspaceKeys(info string) (int64, error) {
	var total int64
	for _, line := range strings.Split(info, "\r\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "db") {
			continue
		}
		_, fields, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		for _, field := range strings.Split(fields, ",") {
			value, found := strings.CutPrefix(field, "keys=")
			if !found {
				continue
			}
			keys, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parse %q: %w", line, err)
			}
			total += keys
		}
	}
	return total, nil
}

// GetClusterInfo get cluster information by checking slot allocation
func (c *service) GetClusterInfo(ctx context.Context) (*ClusterStatus, error) {
	client := c.createClient()
	if client == nil {
		return nil, nil
	}
	defer client.Close()

	slots, err := client.ClusterSlots(ctx).Result()
	if err != nil {
		return nil, err
	}

	status := &ClusterStatus{
		SlotsAssigned: 0,
	}

	for _, slot := range slots {
		if slot.Start <= slot.End && len(slot.Nodes) > 0 {
			status.SlotsAssigned += slot.End - slot.Start + 1
		}
	}

	return status, nil
}
