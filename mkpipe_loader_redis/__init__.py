import gc
import json
from datetime import datetime

from mkpipe.spark.base import BaseLoader
from mkpipe.spark.columns import add_etl_columns
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.utils import get_logger

logger = get_logger(__name__)


class RedisLoader(BaseLoader, variant='redis'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.host = connection.host or 'localhost'
        self.port = connection.port or 6379
        self.password = connection.password
        self.database = int(connection.database or 0)

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        write_mode = data.write_mode
        df = data.df

        if df is None:
            logger.info({'table': target_name, 'status': 'skipped', 'reason': 'no data'})
            return

        df = add_etl_columns(df, datetime.now(), dedup_columns=table.dedup_columns)

        logger.info({
            'table': target_name,
            'status': 'loading',
            'write_mode': write_mode,
        })

        import redis

        r = redis.Redis(
            host=self.host,
            port=self.port,
            password=self.password,
            db=self.database,
            decode_responses=True,
        )

        key_column = self.connection.extra.get('key_column', '_key')
        storage_type = self.connection.extra.get('storage_type', 'hash')
        key_prefix = self.connection.extra.get('key_prefix', f'{target_name}:')
        ttl = self.connection.extra.get('ttl', None)

        if write_mode == 'overwrite':
            pattern = f'{key_prefix}*'
            cursor = 0
            while True:
                cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
                if keys:
                    r.delete(*keys)
                if cursor == 0:
                    break
            logger.info({'table': target_name, 'status': 'keys_deleted', 'pattern': pattern})

        rows = [row.asDict(recursive=True) for row in df.collect()]

        pipe = r.pipeline()
        for i, row in enumerate(rows):
            key_val = row.pop(key_column, None)
            if key_val is None:
                key = f'{key_prefix}{i}'
            else:
                key = f'{key_prefix}{key_val}' if not str(key_val).startswith(key_prefix) else str(key_val)

            if storage_type == 'hash':
                mapping = {k: str(v) if v is not None else '' for k, v in row.items()}
                pipe.hset(key, mapping=mapping)
            else:
                pipe.set(key, json.dumps(row, default=str))

            if ttl:
                pipe.expire(key, int(ttl))

            if (i + 1) % 1000 == 0:
                pipe.execute()
                pipe = r.pipeline()

        pipe.execute()

        df.unpersist()
        gc.collect()

        logger.info({
            'table': target_name,
            'status': 'loaded',
            'rows': len(rows),
        })
