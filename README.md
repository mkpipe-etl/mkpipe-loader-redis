# mkpipe-loader-redis

Redis loader plugin for [MkPipe](https://github.com/mkpipe-etl/mkpipe). Writes Spark DataFrames into Redis using `redis-py` pipeline for efficient batched writes. Supports `hash` and `string` (JSON) storage types.

## Documentation

For more detailed documentation, please visit the [GitHub repository](https://github.com/mkpipe-etl/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

---

## Connection Configuration

```yaml
connections:
  redis_target:
    variant: redis
    host: localhost
    port: 6379
    password: mypassword
    database: "0"
```

---

## Table Configuration

```yaml
pipelines:
  - name: pg_to_redis
    source: pg_source
    destination: redis_target
    tables:
      - name: public.users
        target_name: user
        replication_method: full
```

---

## Write Strategy

Control how data is written to Redis:

```yaml
      - name: public.users
        target_name: user
        write_strategy: upsert       # replace | upsert
```

| Strategy | Redis Behavior |
|---|---|
| `replace` | Delete all keys matching `{key_prefix}*`, then write (default for full). Use `if_exists: append` to skip deletion |
| `upsert` | Write keys directly — `HSET`/`SET` is naturally idempotent, existing keys are overwritten (default for incremental) |

> **Note:** Redis does not support `append` or `merge` strategies. Every write is key-based and naturally idempotent — `upsert` is the default behavior.

---

## Key Structure and Storage Options

Redis keys are constructed as `{key_prefix}{key_value}`. These are configured via `extra` on the connection:

```yaml
connections:
  redis_target:
    variant: redis
    host: localhost
    port: 6379
    password: mypassword
    database: "0"
    extra:
      key_column: id           # column whose value becomes the key suffix (default: _key)
      key_prefix: "user:"      # prepended to every key (default: "{target_name}:")
      storage_type: hash       # 'hash' (default) or 'string' (JSON)
      ttl: 3600                # optional TTL in seconds
```

### Storage types

| `storage_type` | Redis command | Value format |
|---|---|---|
| `hash` (default) | `HSET key field value ...` | Each column is a hash field |
| `string` | `SET key value` | Entire row serialized as JSON string |

---

## Write Throughput

Writes are batched using a Redis pipeline (auto-flushed every 1,000 rows). This gives near-optimal throughput without extra configuration:

```yaml
      - name: public.users
        target_name: user
        replication_method: full
```

### Performance Notes

- Redis pipeline batches commands in memory and sends them in one round-trip per 1,000 rows.
- Redis is CPU-bound on the server side — a single connection is typically sufficient for most write workloads.
- All data is collected on the Spark driver (`df.collect()`) before writing — not suitable for datasets larger than available driver memory.

---

## All Table Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Source table name |
| `target_name` | string | required | Redis key prefix (used as default `key_prefix`) |
| `replication_method` | `full` / `incremental` | `full` | Replication strategy |
| `write_strategy` | string | — | `replace`, `upsert` |
| `if_exists` | string | — | `replace` (delete+write) or `append` (skip deletion). Inherits from settings |
| `dedup_columns` | list | — | Columns used for `mkpipe_id` hash deduplication |
| `tags` | list | `[]` | Tags for selective pipeline execution |
| `pass_on_error` | bool | `false` | Skip table on error instead of failing |

### Extra Connection Parameters

| Key | Default | Description |
|---|---|---|
| `key_column` | `_key` | Column whose value is appended to `key_prefix` to form the Redis key |
| `key_prefix` | `"{target_name}:"` | Prefix for all keys written |
| `storage_type` | `hash` | `hash` or `string` |
| `ttl` | `null` | Key expiry in seconds (not set if omitted) |
