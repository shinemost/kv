use crate::{value, KvError, Kvpair, Storage, Value};
use rocksdb::{Options, DB};
use std::path::Path;
use std::sync::{Arc, RwLock};

#[derive(Debug)]
pub struct Rocksdb(Arc<RwLock<DB>>);

impl Rocksdb {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(Arc::new(RwLock::new(DB::open_default(path).unwrap())))
    }
}

impl Storage for Rocksdb {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let db = self.0.read().unwrap();
        let Some(cf_handle) = db.cf_handle(table) else {
            return Ok(None); // 表不存在直接返回None
        };

        let res = db.get_cf(cf_handle, key.as_bytes());
        match res {
            Ok(None) => Ok(None),
            Ok(Some(x)) => Ok(Some(x.into())),
            Err(e) => Err(KvError::from(e)),
        }
    }

    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError> {
        let mut db = self.0.write().unwrap();

        // 尝试获取现有的CF句柄，如果不存在则创建
        let cf_handle = if let Some(cf) = db.cf_handle(table) {
            cf
        } else {
            // 创建新的Column Family
            db.create_cf(table, &Options::default())
                .map_err(|e| KvError::Internal(format!("Failed to create column family: {}", e)))?;

            // 获取新创建的CF句柄
            db.cf_handle(table).ok_or_else(|| {
                KvError::Internal("Failed to get newly created column family".into())
            })?
        };

        // 先获取旧值（如果存在）
        let old_value = match db.get_cf(cf_handle, key.as_bytes()) {
            Ok(Some(data)) => Some(data.into()),
            Ok(None) => None,
            Err(e) => return Err(KvError::Internal(format!("Get operation failed: {}", e))),
        };

        // 将value序列化为字节
        let data: Vec<u8> = value.try_into().map_err(|e| e)?;

        // 执行put操作
        db.put_cf(cf_handle, key.as_bytes(), &data)
            .map_err(|e| KvError::Internal(format!("Put operation failed: {}", e)))?;

        // 返回旧值（如果是第一次set则返回None）
        Ok(old_value)
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        let db = self.0.read().unwrap(); // 只读锁
        match db.cf_handle(table) {
            Some(cf) => db
                .get_cf(cf, key.as_bytes())
                // 处理上一层 Result 里 OK 的数据
                .map(|v| v.is_some())
                // 处理 Error
                .map_err(|e| e.into()),
            // table 都不存在肯定也不包含了
            None => Ok(false),
        }
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let db = self.0.read().unwrap();
        let Some(cf) = db.cf_handle(table) else {
            // 表不存在直接返回None
            return Ok(None);
        };

        // 先获取旧值
        let old_value = match db.get_cf(cf, key.as_bytes())? {
            Some(data) => Some(data.into()),
            None => None,
        };

        // 执行删除
        db.delete_cf(cf, key.as_bytes())
            .map_err(|e| KvError::from(e))?;

        Ok(old_value)
    }

    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError> {
        let db = self.0.read().unwrap();
        let Some(cf) = db.cf_handle(table) else {
            return Ok(vec![]); // 表不存在返回空vec
        };

        let iter = db.iterator_cf(cf, rocksdb::IteratorMode::Start);

        // 对迭代器每个元素执行闭包函数
        iter.map(|item| {
            let (key, value) = item?; // 自动转换rocksdb::Error
            Ok(Kvpair {
                key: String::from_utf8_lossy(&key).into_owned(),
                value: Some(Value::from(value.to_vec())),
            })
        })
        .collect()
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError> {
        let pairs = self.get_all(table)?; // 先获取所有数据
        Ok(Box::new(pairs.into_iter())) // 转换为owned迭代器
    }
}

impl From<rocksdb::Error> for KvError {
    fn from(error: rocksdb::Error) -> Self {
        KvError::Internal(error.to_string())
    }
}

impl From<Vec<u8>> for Value {
    fn from(data: Vec<u8>) -> Self {
        Self {
            value: Some(value::Value::String(
                String::from_utf8_lossy(&data)
                    .trim_matches(|c: char| c.is_control())
                    .to_owned(),
            )),
        }
    }
}
