use crate::*;

impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hgetall {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        // match store.get_all(&self.table) {
        //     Ok(v) => v.into(),
        //     Err(e) => e.into(),
        // }
        // 使用迭代器是否更好？
        store.get_iter(&self.table).unwrap().collect::<Vec<_>>().into()
    }
}

impl CommandService for Hset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match self.pair {
            Some(v) => match store.set(&self.table, v.key, v.value.unwrap_or_default()) {
                Ok(Some(v)) => v.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            },
            None => Value::default().into(),
        }
    }
}

impl CommandService for Hmget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        // let mut list:Vec<Value> = vec![];
        // for x in &self.keys {
        //     match store.get(&self.table,x) {
        //         Ok(Some(v)) => list.push(v.into()),
        //         Ok(None) => list.push(Value::default().into()),
        //         Err(e) => list.push(e.into()),
        //     }
        // }
        // list.into()
        // 使用迭代器比循环更好
        self.keys
            .iter()
            .map(|key| match store.get(&self.table, key) {
                Ok(Some(v)) => v,
                _ => Value::default(),
            })
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hmset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        // let mut list:Vec<Value> = vec![];
        // for pair in self.pairs {
        //     match store.set(&self.table, pair.key, pair.value.unwrap_or_default()) {
        //         Ok(Some(v)) => list.push(v.into()),
        //         _ => list.push(Value::default().into()),
        //     }
        // }
        // list.into()
        let table = self.table;
        let pairs = self.pairs;
        // into_iter会拿走 pairs的所有权
        pairs
            .into_iter()
            .map(
                |pair| match store.set(&table, pair.key, pair.value.unwrap_or_default()) {
                    Ok(Some(v)) => v,
                    _ => Value::default(),
                },
            )
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.del(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let keys = self.keys;
        keys.into_iter()
            .map(|key| match store.del(&self.table, &key) {
                Ok(Some(v)) => v.into(),
                Ok(None) => KvError::NotFound(self.table.clone(), key).into(),
                Err(e) => e.into(),
            })
            .collect::<Vec<Value>>()
            .into()
    }
}

impl CommandService for Hexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.contains(&self.table, &self.key) {
            Ok(v) => Value::from(v).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        let keys = self.keys;
        keys.into_iter()
            .map(|key| match store.contains(&self.table, &key) {
                Ok(v) => v.into(),
                Err(e) => e.into(),
            })
            .collect::<Vec<Value>>()
            .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_request::RequestData;

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("t1", "hello", "world".into());
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &[Value::default()], &[]);
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["world".into()], &[]);
    }

    #[test]
    fn hget_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        dispatch(cmd, &store);
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[10.into()], &[]);
    }
    #[test]
    fn hget_with_non_exist_key_should_return_404() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_error(res, 404, "Not found");
    }

    #[test]
    fn hgetall_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "u1", 10.into()),
            CommandRequest::new_hset("score", "u2", 8.into()),
            CommandRequest::new_hset("score", "u3", 11.into()),
            CommandRequest::new_hset("score", "u1", 6.into()),
        ];
        for cmd in cmds {
            dispatch(cmd, &store);
        }
        let cmd = CommandRequest::new_hgetall("score");
        let res = dispatch(cmd, &store);
        let pairs = &[
            Kvpair::new("u1", 6.into()),
            Kvpair::new("u2", 8.into()),
            Kvpair::new("u3", 11.into()),
        ];
        assert_res_ok(res, &[], pairs);
    }

    #[test]
    fn hmget_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "u1", 10.into()),
            CommandRequest::new_hset("score", "u2", 8.into()),
            CommandRequest::new_hset("score", "u3", 11.into()),
        ];
        for cmd in cmds {
            dispatch(cmd, &store);
        }
        let v = vec!["u1".to_string(), "u2".to_string()];
        let cmd = CommandRequest::new_hmget("score", v);
        let res = dispatch(cmd, &store);
        let values = &[10.into(), 8.into()];
        assert_res_ok(res, values, &[]);
    }

    #[test]
    fn hmget_with_non_exist_key_should_return_404() {
        let store = MemTable::new();
        let v = vec!["u1".to_string(), "u2".to_string()];
        let cmd = CommandRequest::new_hmget("score", v);
        let res = dispatch(cmd, &store);
        let values = &[Value::default(), Value::default()];
        assert_res_ok(res, values, &[]);
    }

    #[test]
    fn hmset_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hmset(
            "t1",
            vec![
                Kvpair::new("u1", Value::from("hello")),
                Kvpair::new("u2", Value::from("world")),
            ],
        );
        let res = dispatch(cmd.clone(), &store);
        assert_res_ok(res, &[Value::default(), Value::default()], &[]);
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["hello".into(), "world".into()], &[]);
    }

    #[test]
    fn hdel_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("score", "u1", "hello".into());
        _ = dispatch(cmd, &store);
        let cmd2 = CommandRequest::new_hdel("score", "u1");
        let res = dispatch(cmd2, &store);
        assert_res_ok(res, &["hello".into()], &[]);
    }

    #[test]
    fn hdel_with_non_exist_key_should_return_404() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hdel("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_error(res, 404, "Not found");
    }

    #[test]
    fn hmdel_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hmset(
            "t1",
            vec![
                Kvpair::new("u1", Value::from("hello")),
                Kvpair::new("u2", Value::from("world")),
            ],
        );
        _ = dispatch(cmd.clone(), &store);

        let cmd2 = CommandRequest::new_hmdel("t1", vec!["u1".to_string(), "u2".to_string()]);
        let res = dispatch(cmd2, &store);
        assert_res_ok(res, &["hello".into(), "world".into()], &[]);
    }

    #[test]
    fn hexist_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        _ = dispatch(cmd.clone(), &store);
        let cmd2 = CommandRequest::new_hexist("score", "u1");
        let res = dispatch(cmd2, &store);
        assert_res_ok(res, &[true.into()], &[]);
    }

    #[test]
    fn hmexist_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hmset(
            "t1",
            vec![
                Kvpair::new("u1", Value::from("hello")),
                Kvpair::new("u2", Value::from("world")),
            ],
        );
        _ = dispatch(cmd.clone(), &store);
        let cmd2 = CommandRequest::new_hmexist(
            "t1",
            vec!["u1".to_string(), "u2".to_string(), "u3".to_string()],
        );
        let res = dispatch(cmd2, &store);
        assert_res_ok(res, &[true.into(), true.into(), false.into()], &[]);
    }

    // 从 Request 中得到 Response，目前处理 HGET/HGETALL/HSET
    fn dispatch(cmd: CommandRequest, store: &impl Storage) -> CommandResponse {
        match cmd.request_data.unwrap() {
            RequestData::Hget(v) => v.execute(store),
            RequestData::Hgetall(v) => v.execute(store),
            RequestData::Hset(v) => v.execute(store),
            RequestData::Hmget(v) => v.execute(store),
            RequestData::Hmset(v) => v.execute(store),
            RequestData::Hdel(v) => v.execute(store),
            RequestData::Hmdel(v) => v.execute(store),
            RequestData::Hexist(v) => v.execute(store),
            RequestData::Hmexist(v) => v.execute(store),
        }
    }

    // 测试成功返回的结果
    fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[Kvpair]) {
        res.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(res.status, 200);
        assert_eq!(res.message, "");
        assert_eq!(res.values, values);
        assert_eq!(res.pairs, pairs);
    }
    // 测试失败返回的结果
    fn assert_res_error(res: CommandResponse, code: u32, msg: &str) {
        assert_eq!(res.status, code);
        assert!(res.message.contains(msg));
        assert_eq!(res.values, &[]);
        assert_eq!(res.pairs, &[]);
    }
}
