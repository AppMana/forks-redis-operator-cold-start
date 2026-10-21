# frozen_string_literal: true

# Run only in a disposable lab Job using the exact Lago application image.
# Tests real client libraries/configuration, not Rails jobs or billing semantics.
require "bundler/setup"
require "openssl"
require "redis"
require "sidekiq/redis_client_adapter"
$LOAD_PATH.unshift("/app/lib")
require "lago/redis_config_builder"
require "securerandom"

$stdout.sync = true
namespace = "redis-upgrade-canary-empty-disk"
raise "lab opt-in required" unless ENV["CANARY_LAGO_TEST"] == namespace
mode = ENV.fetch("CANARY_LAGO_MODE")
read_only = ENV["CANARY_LAGO_READ_ONLY"] == "1"
raise "unsupported mode" unless %w[service sentinel].include?(mode)
host = "upgrade-master.#{namespace}.svc.cluster.local"
ENV["REDIS_URL"] = "redis://#{host}:6379/0"
ENV["LAGO_REDIS_CACHE_URL"] = "redis://#{host}:6379/1"
%w[REDIS_PASSWORD LAGO_REDIS_CACHE_PASSWORD].each { |key| ENV.delete(key) }
%w[SIDEKIQ CACHE].each do |kind|
  prefix = "LAGO_REDIS_#{kind}"
  %w[SENTINELS SENTINEL_USERNAME SENTINEL_PASSWORD MASTER_NAME].each do |suffix|
    ENV.delete("#{prefix}_#{suffix}")
  end
  next unless mode == "sentinel"

  ENV["#{prefix}_SENTINELS"] = (0..2).map do |index|
    "upgrade-sentinel-#{index}.upgrade-sentinel-headless.#{namespace}.svc.cluster.local:26379"
  end.join(",")
  ENV["#{prefix}_MASTER_NAME"] = "upgrade"
end

builder = Lago::RedisConfigBuilder.new
clients = {
  sidekiq: Sidekiq::RedisClientAdapter.new(builder.sidekiq).new_client,
  cache: Redis.new(builder.cache)
}
clock = -> { Process.clock_gettime(Process::CLOCK_MONOTONIC) }
threads = clients.map do |kind, client|
  Thread.new do
    prefix = "appmana:canary:lago:#{mode}:#{kind}:#{SecureRandom.hex(12)}"
    retained = "preserved-empty-disk-20261021"
    retained_key = kind == :cache ? "appmana:upgrade:secondary-database" : "appmana:upgrade:sentinel-regression"
    acknowledgements = {}
    process_ids = {}
    errors = Hash.new(0)
    start = clock.call
    last_success = start
    sequence = 0
    reads = 0
    while clock.call - start < 120
      begin
        raise "retained value mismatch" unless client.get(retained_key) == retained
        unless read_only
          key = "#{prefix}:#{sequence}"
          value = "value-#{sequence}"
          sequence += 1
          raise "SET not acknowledged" unless client.set(key, value) == "OK"
          acknowledgements[key] = value
          raise "read-after-write mismatch" unless client.get(key) == value
        end
        info = client.info
        process_ids[info.fetch("run_id")] = true
        if info.fetch("role") == "master"
          reads += 1
          last_success = clock.call
          puts "READY #{mode}/#{kind} read_only=#{read_only} retained_prefix=#{prefix}" if reads == 1
        else
          errors["NonPrimaryRead"] += 1
        end
      rescue Redis::BaseError, RedisClient::Error => error
        # Do not log URLs or credentials. Failed writes may be ambiguous.
        errors[error.class.name] += 1
      end
      if clock.call - last_success > 30
        raise "#{mode}/#{kind}: client recovery exceeded 30 seconds; read_only=#{read_only}, processes=#{process_ids.size}, errors=#{errors}"
      end
      sleep 0.2
    end
    raise "no observed primary change" unless process_ids.size >= 2
    raise "insufficient acknowledged writes" if !read_only && acknowledgements.size < 20
    raise "insufficient primary reads" if reads < 20
    raise "client unhealthy at end" if clock.call - last_success > 2
    missing = acknowledgements.filter_map do |key, value|
      key.delete_prefix("#{prefix}:") unless client.get(key) == value
    end
    unless missing.empty?
      puts "FAIL #{mode}/#{kind}: acknowledged=#{acknowledgements.size}, missing_or_changed=#{missing.size}, indices=#{missing.join(',')}, processes=#{process_ids.size}, errors=#{errors}, retained_prefix=#{prefix}"
      raise "acknowledged write missing or changed"
    end
    puts "PASS #{mode}/#{kind}: read_only=#{read_only}, reads=#{reads}, acknowledged=#{acknowledgements.size}, processes=#{process_ids.size}, errors=#{errors}, retained_prefix=#{prefix}"
  ensure
    client.close
  end
end
threads.each(&:value)
