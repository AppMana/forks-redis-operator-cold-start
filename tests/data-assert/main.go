package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"strings"
	"text/template"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
)

// go run main.go gen-resource-yaml
// go run main.go gen-redis-data --host redis-cluster-0.redis-cluster.default.svc.cluster.local --password 123456 --mode cluster
// go run main.go gen-redis-data --host sentinel-0.sentinel.default.svc.cluster.local --password 123456 --mode sentinel --password-sentinel Opstree@1234sentinel --master-name mymaster
// go run main.go chk-redis-data --host redis-cluster-0.redis-cluster.default.svc.cluster.local --password 123456 --mode cluster
// go run main.go chk-redis-data --host sentinel-0.sentinel.default.svc.cluster.local --password 123456 --mode sentinel --password-sentinel Opstree@1234sentinel --master-name mymaster

const (
	hostFlag         = "host"
	passFlag         = "password"
	sentinelPassFlag = "password-sentinel"
	modeFlag         = "mode"
	masterNameFlag   = "master-name"
	totalKey         = 1000
)

var (
	host       string
	pass       string
	mode       string
	tlsFlag    bool
	masterName string

	sentinelPass string
)

func main() {
	if err := newRootCommand().Execute(); err != nil {
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "data-assert",
		SilenceUsage: true,
	}
	rootCmd.AddCommand(&cobra.Command{
		Use: "gen-resource-yaml",
		Run: genResourceYamlCmd,
	})
	rootCmd.AddCommand(&cobra.Command{
		Use:  "gen-redis-data",
		RunE: genRedisDataCmd,
	})
	rootCmd.AddCommand(&cobra.Command{
		Use:  "chk-redis-data",
		RunE: chkRedisDataCmd,
	})

	// add flags
	rootCmd.PersistentFlags().StringVarP(&host, hostFlag, "H", "", "redis host")
	rootCmd.PersistentFlags().StringVarP(&pass, passFlag, "P", "", "redis password")
	rootCmd.PersistentFlags().StringVarP(&mode, modeFlag, "M", "", "redis mode")
	rootCmd.PersistentFlags().BoolVarP(&tlsFlag, "tls", "T", false, "enable tls")
	rootCmd.PersistentFlags().StringVarP(&sentinelPass, sentinelPassFlag, "S", "", "redis sentinel password")
	rootCmd.PersistentFlags().StringVar(&masterName, masterNameFlag, "myMaster", "redis sentinel master name")
	return rootCmd
}

func genRedisDataCmd(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	// Split host string by comma
	hosts := strings.Split(host, ",")
	for i := range hosts {
		hosts[i] = strings.TrimSpace(hosts[i])
	}

	rdb, err := createRedisClient(mode, hosts, pass, sentinelPass, masterName, tlsFlag)
	if err != nil {
		return fmt.Errorf("failed to create redis client: %w", err)
	}
	defer rdb.Close()

	// Generate and write data
	for i := range totalKey {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		err := rdb.Set(ctx, key, value, 0).Err()
		if err != nil {
			return fmt.Errorf("failed to set key %s: %w", key, err)
		}
	}
	fmt.Fprintf(cmd.OutOrStdout(), "[OK] successfully generated %d keys\n", totalKey)
	return nil
}

// DataError represents data consistency check errors
type DataError struct {
	ExpectedCount int // Expected number of keys
	ActualCount   int // Actual number of keys found
}

func (e *DataError) Error() string {
	return fmt.Sprintf("\nData count mismatch:\n  - Expected: %d keys\n  - Actual: %d keys\n  - Missing: %d keys",
		e.ExpectedCount, e.ActualCount, e.ExpectedCount-e.ActualCount)
}

func chkRedisDataCmd(cmd *cobra.Command, args []string) error {
	if err := checkRedisData(cmd.Context()); err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "[OK] Data consistency check passed! All %d keys and values match\n", totalKey)
	return nil
}

func checkRedisData(ctx context.Context) error {
	// Split host string by comma
	hosts := strings.Split(host, ",")
	for i := range hosts {
		hosts[i] = strings.TrimSpace(hosts[i])
	}

	rdb, err := createRedisClient(mode, hosts, pass, sentinelPass, masterName, tlsFlag)
	if err != nil {
		return fmt.Errorf("failed to create redis client: %w", err)
	}
	defer rdb.Close()

	return checkData(ctx, rdb)
}

type dataReader interface {
	Get(context.Context, string) *redis.StringCmd
}

func checkData(ctx context.Context, rdb dataReader) error {
	// A surviving key with a changed value is data loss too.
	actualCount := 0
	for i := range totalKey {
		key := fmt.Sprintf("key-%d", i)
		value, err := rdb.Get(ctx, key).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return fmt.Errorf("error checking key %s: %w", key, err)
		}
		if value != fmt.Sprintf("value-%d", i) {
			return fmt.Errorf("data value mismatch for key %s", key)
		}
		actualCount++
	}

	if actualCount != totalKey {
		return &DataError{
			ExpectedCount: totalKey,
			ActualCount:   actualCount,
		}
	}
	return nil
}

func createRedisClient(mode string, hosts []string, pass string, sentinelPass string, masterName string, tlsFlag bool) (redis.UniversalClient, error) {
	switch mode {
	case "cluster":
		opts := &redis.ClusterOptions{
			Addrs:    hosts,
			Password: pass,
		}
		if tlsFlag {
			opts.TLSConfig = &tls.Config{InsecureSkipVerify: true}
		}
		return redis.NewClusterClient(opts), nil
	case "sentinel":
		opts := &redis.FailoverOptions{
			MasterName:       masterName,
			SentinelAddrs:    hosts,
			Password:         pass,
			SentinelPassword: sentinelPass,
		}
		if tlsFlag {
			opts.TLSConfig = &tls.Config{InsecureSkipVerify: true}
		}
		return redis.NewFailoverClient(opts), nil
	default:
		return nil, fmt.Errorf("unsupported redis mode: %s", mode)
	}
}

func genResourceYamlCmd(cmd *cobra.Command, args []string) {
	mainGoBytes, err := os.ReadFile("main.go")
	if err != nil {
		panic(err)
	}
	indentedMain := "    " + strings.Join(strings.Split(string(mainGoBytes), "\n"), "\n    ")

	goModBytes, err := os.ReadFile("go.mod")
	if err != nil {
		panic(err)
	}
	goModContent := "    " + strings.Join(strings.Split(string(goModBytes), "\n"), "\n    ")

	goSumBytes, err := os.ReadFile("go.sum")
	if err != nil {
		panic(err)
	}
	goSumContent := "    " + strings.Join(strings.Split(string(goSumBytes), "\n"), "\n    ")

	outFile, err := os.Create("resources.yaml")
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	err = template.Must(template.ParseFiles("resources.yaml.tmpl")).Execute(outFile, map[string]string{
		"Main":   indentedMain,
		"GoMod":  goModContent,
		"GoSum":  goSumContent,
		"Notice": "## DO NOT MODIFY THIS FILE!!!, IT IS GENERATED FROM resources.yaml.tmpl",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("✅resources.yaml generated")
}
