use deadpool_redis::{Config, Pool, Runtime, Connection};
use redis::Script;
use shared::types::Entry;
use tracing::info;

pub struct RedisStore {
    pool: Pool,
}

impl RedisStore {
    pub fn new(redis_url: &str) -> Result<Self, deadpool_redis::CreatePoolError> {
        let cfg = Config::from_url(redis_url);
        let pool = cfg.create_pool(Some(Runtime::Tokio1))?;
        info!("connected to Redis at {}", redis_url);
        Ok(Self { pool })
    }

    async fn get_conn(&self) -> Result<Connection, Box<dyn std::error::Error>> {
        Ok(self.pool.get().await?)
    }

    
    pub async fn get(&self, key: &str) -> Result<Option<Entry>, Box<dyn std::error::Error>> {
        let mut conn = self.get_conn().await?;

        let bytes: Option<Vec<u8>> = redis::cmd("GET")
            .arg(key)
            .query_async(&mut *conn)
            .await?;

        match bytes {
            Some(b) => {
                let entry: Entry = bincode::deserialize(&b)?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    // ── Set (Atomic Lua Script) ───────────────────────────────
    pub async fn set_atomic(
        &self,
        incoming: &Entry,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.get_conn().await?;
        let incoming_bytes = bincode::serialize(incoming)?;

        let script = Script::new(r#"
            local existing = redis.call('GET', KEYS[1])
            if existing == false then
                redis.call('SET', KEYS[1], ARGV[1])
                return 1
            end
            redis.call('SET', KEYS[1], ARGV[1])
            return 1
        "#);

        script
            .key(&incoming.key)
            .arg(incoming_bytes)
            .invoke_async::<_, ()>(&mut *conn)
            .await?;

        Ok(())
    }

    // ── Delete ───────────────────────────────────────────────
    pub async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.get_conn().await?;

        redis::cmd("DEL")
            .arg(key)
            .query_async::<_, ()>(&mut *conn)
            .await?;

        Ok(())
    }

    // ── All entries ───────────────────────────────────────────
    pub async fn all_entries(&self) -> Result<Vec<Entry>, Box<dyn std::error::Error>> {
        let mut conn = self.get_conn().await?;

        let keys: Vec<String> = redis::cmd("KEYS")
            .arg("rates:*")
            .query_async(&mut *conn)
            .await?;

        let mut entries = Vec::new();
        for key in keys {
            if let Ok(Some(entry)) = self.get(&key).await {
                entries.push(entry);
            }
        }

        Ok(entries)
    }

    // ── Publish ───────────────────────────────────────────────
    pub async fn publish(
        &self,
        entry: &Entry,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut conn = self.get_conn().await?;
        let bytes = bincode::serialize(entry)?;

        redis::cmd("PUBLISH")
            .arg("rate-updates")
            .arg(bytes)
            .query_async::<_, ()>(&mut *conn)
            .await?;

        Ok(())
    }
}