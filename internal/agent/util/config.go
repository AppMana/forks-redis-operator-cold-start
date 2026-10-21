package util

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/OT-CONTAINER-KIT/redis-operator/internal/util"
)

type Config struct {
	content string
	path    string
}

func NewConfig(path string, defaultConfig ...string) *Config {
	return &Config{
		path:    path,
		content: strings.Join(defaultConfig, "\n"),
	}
}

// configValueSanitizer strips the line-breaking characters (CR/LF) that would
// let a value escape its directive line. Redis only splits config into
// directives on '\n' and only treats '#' as a comment at the start of a line,
// and these values are always written after a fixed directive token, so
// stripping CR/LF is sufficient to prevent directive injection.
var configValueSanitizer = strings.NewReplacer(
	"\r", "",
	"\n", "",
)

// SanitizeConfigValue removes characters that would let a value escape its
// directive line. Cleaning rather than erroring keeps the bootstrap path
// resilient when upstream CRD validation is bypassed (e.g. existing objects,
// partial RBAC).
func SanitizeConfigValue(s string) string {
	return configValueSanitizer.Replace(s)
}

// QuoteConfigArgument encodes one redis.conf argument (not an ACL-file rule).
// Redis accepts byte escapes, not Go's Unicode escapes. Preserve significant
// whitespace while retaining the existing CR/LF removal contract.
func QuoteConfigArgument(value string) string {
	value = SanitizeConfigValue(value)
	quote := value == ""
	for i := 0; i < len(value); i++ {
		b := value[i]
		quote = quote || b <= ' ' || b == 127 || b == '\'' || b == '"' || b == '\\'
	}
	if !quote {
		return value
	}
	var encoded strings.Builder
	encoded.WriteByte('"')
	for i := 0; i < len(value); i++ {
		b := value[i]
		switch {
		case b == '"' || b == '\\':
			encoded.WriteByte('\\')
			encoded.WriteByte(b)
		case b < ' ' || b == 127:
			fmt.Fprintf(&encoded, "\\x%02x", b)
		default:
			encoded.WriteByte(b)
		}
	}
	encoded.WriteByte('"')
	return encoded.String()
}

func (c *Config) Append(config ...string) *Config {
	if len(config) == 0 {
		return c
	}
	directive := SanitizeConfigValue(config[0])
	args := make([]string, 0, len(config)-1)
	for _, a := range config[1:] {
		args = append(args, SanitizeConfigValue(a))
	}
	c.content = fmt.Sprintf("%s\n%s %s", c.content, directive, strings.Join(args, " "))
	return c
}

func (c *Config) Commit() error {
	dir := filepath.Dir(c.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", dir, err)
	}
	return os.WriteFile(c.path, []byte(c.content), 0o644)
}

// CommitAtomic never truncates the only persisted copy of an election.
func (c *Config) CommitAtomic() error {
	dir := filepath.Dir(c.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".sentinel-config-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.WriteString(c.content); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), c.path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (c *Config) AppendExternalConfig(externalConfigFile string, expand bool) *Config {
	if _, err := os.Stat(externalConfigFile); err != nil {
		return c
	}
	includeFile := externalConfigFile
	if expand {
		expandedFile, err := ExpandExternalConfig(externalConfigFile, c.path)
		if err != nil {
			log.Printf("Warning: Failed to expand external config %s, including verbatim: %v", externalConfigFile, err)
		} else {
			includeFile = expandedFile
		}
	}
	return c.Append("include", includeFile)
}

func ExpandExternalConfig(externalConfigFile, confPath string) (string, error) {
	raw, err := os.ReadFile(externalConfigFile)
	if err != nil {
		return "", err
	}
	expanded := os.Expand(string(raw), func(key string) string {
		return util.CoalesceEnv1(key, "")
	})

	base := strings.TrimSuffix(filepath.Base(externalConfigFile), ".conf")
	expandedFile := filepath.Join(filepath.Dir(confPath), base+".expanded.conf")
	if err := os.WriteFile(expandedFile, []byte(expanded), 0o644); err != nil {
		return "", fmt.Errorf("write expanded config to %s: %v", expandedFile, err)
	}
	return expandedFile, nil
}
