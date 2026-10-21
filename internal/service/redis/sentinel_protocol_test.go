package redis

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	rediscli "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// Use actual wire replies, not mocked command values: a RESP3 map read as a
// slice consumes only half the fields and corrupts the next command's reply.
func TestSentinelMonitorWireProtocols(t *testing.T) {
	for _, protocol := range []int{2, 3} {
		t.Run(fmt.Sprint(protocol), func(t *testing.T) {
			listenConfig := net.ListenConfig{}
			listener, err := listenConfig.Listen(context.Background(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { _ = listener.Close() })
			finished := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					finished <- err
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				reader := bufio.NewReader(conn)
				var commands []string
				for {
					args, err := readTestCommand(reader)
					if err != nil {
						finished <- err
						return
					}
					switch strings.ToLower(args[0]) {
					case "hello":
						_, _ = io.WriteString(conn, "%1\r\n+proto\r\n:"+strconv.Itoa(protocol)+"\r\n")
					case "client":
						_, _ = io.WriteString(conn, "+OK\r\n")
					case "sentinel":
						verb := strings.ToUpper(args[1])
						commands = append(commands, verb)
						switch verb {
						case "MASTER":
							writeTestMap(conn, protocol, []string{"ip", "10.0.0.1", "port", "6379", "flags", "master", "quorum", "2"})
						case "CONFIG":
							writeTestMap(conn, protocol, []string{"resolve-hostnames", "yes"})
						case "REMOVE":
							_, _ = io.WriteString(conn, "+OK\r\n")
						case "MONITOR":
							_, _ = io.WriteString(conn, "+OK\r\n")
						case "SET":
							_, _ = io.WriteString(conn, "+OK\r\n")
							if strings.Join(commands, ",") != "MASTER,CONFIG,REMOVE,MONITOR,SET" {
								finished <- fmt.Errorf("unexpected sequence %v", commands)
							} else {
								finished <- nil
							}
							return
						default:
							finished <- fmt.Errorf("unexpected Sentinel verb %s", verb)
							return
						}
					default:
						finished <- fmt.Errorf("unexpected command %s", args[0])
						return
					}
				}
			}()
			db := rediscli.NewClient(&rediscli.Options{Addr: listener.Addr().String(), Protocol: protocol, MaxRetries: -1, ReadTimeout: time.Second, WriteTimeout: time.Second})
			svc := &service{redisClient: db}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			require.NoError(t, svc.SentinelMonitor(ctx, &ConnectionInfo{Host: "redis-0.example", Port: "6379"}, "upgrade", "2"))
			require.NoError(t, <-finished)
		})
	}
}

func readTestCommand(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected command array")
	}
	count, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	var args []string
	for i := 0; i < count; i++ {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(line, "$") {
			return nil, fmt.Errorf("expected bulk argument")
		}
		size, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil || size < 0 || size > 4096 {
			return nil, fmt.Errorf("invalid bulk argument length")
		}
		buf := make([]byte, size+2)
		if _, err = io.ReadFull(reader, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

func writeTestMap(writer io.Writer, protocol int, fields []string) {
	if protocol == 3 {
		_, _ = fmt.Fprintf(writer, "%%%d\r\n", len(fields)/2)
	} else {
		_, _ = fmt.Fprintf(writer, "*%d\r\n", len(fields))
	}
	for _, field := range fields {
		_, _ = fmt.Fprintf(writer, "$%d\r\n%s\r\n", len(field), field)
	}
}
