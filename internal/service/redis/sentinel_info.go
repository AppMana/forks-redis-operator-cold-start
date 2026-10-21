package redis

import (
	"errors"
	"strconv"
	"strings"
)

// Counts drive topology repair. Unknown or incomplete observations must never
// become zero counts or a successful empty inventory. Errors omit reply data.
func parseSentinelInfo(raw string) (*InfoSentinelResult, error) {
	info := &InfoSentinelResult{}
	var declared uint64
	foundCount := false
	indices, names := map[uint64]bool{}, map[string]bool{}
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSuffix(line, "\r")
		key, value, hasColon := strings.Cut(line, ":")
		if key == "sentinel_masters" {
			count, err := strconv.ParseUint(value, 10, 31)
			if !hasColon || foundCount || err != nil {
				return nil, errors.New("invalid or duplicate Sentinel master count")
			}
			declared, foundCount = count, true
			continue
		}
		if !strings.HasPrefix(key, "master") {
			continue
		}
		index, err := strconv.ParseUint(strings.TrimPrefix(key, "master"), 10, 31)
		if !hasColon || err != nil || indices[index] {
			return nil, errors.New("invalid or duplicate Sentinel master entry")
		}
		indices[index] = true
		fields := map[string]string{}
		for _, pair := range strings.Split(value, ",") {
			field, content, ok := strings.Cut(pair, "=")
			_, duplicate := fields[field]
			if !ok || field == "" || duplicate {
				return nil, errors.New("invalid or duplicate Sentinel master field")
			}
			fields[field] = content
		}
		if fields["name"] == "" || fields["status"] == "" || fields["address"] == "" || names[fields["name"]] {
			return nil, errors.New("incomplete or ambiguous Sentinel master identity")
		}
		names[fields["name"]] = true
		slaves, err := strconv.ParseUint(fields["slaves"], 10, 31)
		if err != nil {
			return nil, errors.New("invalid or missing Sentinel replica count")
		}
		sentinels, err := strconv.ParseUint(fields["sentinels"], 10, 31)
		if err != nil {
			return nil, errors.New("invalid or missing Sentinel peer count")
		}
		info.Masters = append(info.Masters, SentinelMasterInfo{Name: fields["name"], Status: fields["status"], Address: fields["address"], Slaves: int(slaves), Sentinels: int(sentinels)})
	}
	if !foundCount || declared != uint64(len(info.Masters)) {
		return nil, errors.New("incomplete Sentinel master inventory")
	}
	for index := range indices {
		if index >= declared {
			return nil, errors.New("invalid Sentinel master inventory index")
		}
	}
	return info, nil
}
