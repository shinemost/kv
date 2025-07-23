use crate::*;
use std::sync::Arc;
use tracing::debug;
mod command_service;

pub trait CommandService {
    fn execute(self, store: &dyn Storage) -> CommandResponse;
}

pub struct Service {
    // inner: Arc<ServiceInner<Store>>,
    pub store: Arc<dyn Storage>,
    on_received: Vec<fn(&CommandRequest) -> Option<CommandResponse>>,
    on_executed: Vec<fn(&CommandResponse) -> Option<CommandResponse>>,
    on_before_send: Vec<fn(&mut CommandResponse) -> Option<CommandResponse>>,
    on_after_send: Vec<fn() -> Option<CommandResponse>>,
}

impl Clone for Service {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            on_received: self.on_received.clone(),
            on_executed: self.on_executed.clone(),
            on_before_send: self.on_before_send.clone(),
            on_after_send: self.on_after_send.clone(),
        }
    }
}

/// Service 内部数据结构
// pub struct ServiceInner<Store> {
//     store: Store,
// }

impl Service {
    pub fn new<S: Storage>(store: S) -> Self {
        Self {
            store: Arc::new(store),
            on_received: Vec::new(),
            on_executed: Vec::new(),
            on_before_send: Vec::new(),
            on_after_send: Vec::new(),
        }
    }

    pub fn execute(&self, cmd: CommandRequest) -> CommandResponse {
        debug!("Got request: {:?}", cmd);

        // 处理接收通知，如果返回 Some，则提前结束
        if let Some(res) = self.on_received.notify(&cmd) {
            return res;
        }
        // self.store.deref()解引用 Arc<dyn Storage> -> &dyn Storage
        // self.store.as_ref()同理
        let mut res = dispatch(cmd, self.store.as_ref());
        debug!("Executed response: {:?}", res);

        // 处理执行通知，如果返回 Some，则提前结束
        if let Some(new_res) = self.on_executed.notify(&res) {
            return new_res;
        }

        // 处理发送前通知，如果返回 Some，则提前结束
        if let Some(new_res) = self.on_before_send.notify(&mut res) {
            return new_res;
        }

        if !self.on_before_send.is_empty() {
            debug!("Modified response: {:?}", res);
        }
        res
    }

    // 修改注册方法，使用新的函数签名
    pub fn fn_received(mut self, f: fn(&CommandRequest) -> Option<CommandResponse>) -> Self {
        self.on_received.push(f);
        self
    }

    pub fn fn_executed(mut self, f: fn(&CommandResponse) -> Option<CommandResponse>) -> Self {
        self.on_executed.push(f);
        self
    }

    pub fn fn_before_send(
        mut self,
        f: fn(&mut CommandResponse) -> Option<CommandResponse>,
    ) -> Self {
        self.on_before_send.push(f);
        self
    }

    pub fn fn_after_send(mut self, f: fn() -> Option<CommandResponse>) -> Self {
        self.on_after_send.push(f);
        self
    }
}

// 从 Request 中得到 Response，目前处理 HGET/HGETALL/HSET
pub fn dispatch(cmd: CommandRequest, store: &dyn Storage) -> CommandResponse {
    match cmd.request_data {
        Some(RequestData::Hget(param)) => param.execute(store),
        Some(RequestData::Hgetall(param)) => param.execute(store),
        Some(RequestData::Hset(param)) => param.execute(store),
        None => KvError::InvalidCommand("Request has no data".into()).into(),
        _ => KvError::Internal("Not implemented".into()).into(),
    }
}

/// 事件通知（不可变事件）
pub trait Notify<Arg> {
    fn notify(&self, arg: &Arg) -> Option<CommandResponse>;
}

/// 事件通知（可变事件）
pub trait NotifyMut<Arg> {
    fn notify(&self, arg: &mut Arg) -> Option<CommandResponse>;
}

impl<Arg> Notify<Arg> for Vec<fn(&Arg) -> Option<CommandResponse>> {
    #[inline]
    fn notify(&self, arg: &Arg) -> Option<CommandResponse> {
        for f in self {
            if let Some(res) = f(arg) {
                return Some(res);
            }
        }
        None
    }
}

impl<Arg> NotifyMut<Arg> for Vec<fn(&mut Arg) -> Option<CommandResponse>> {
    #[inline]
    fn notify(&self, arg: &mut Arg) -> Option<CommandResponse> {
        for f in self {
            if let Some(res) = f(arg) {
                return Some(res);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemTable, Value};
    use http::StatusCode;
    use std::thread;
    use tracing::info;

    #[test]
    fn service_should_works() {
        // 我们需要一个 service 结构至少包含 Storage
        let service = Service::new(MemTable::default());

        // service 可以运行在多线程环境下，它的 clone 应该是轻量级的
        let cloned = service.clone();

        // 创建一个线程，在 table t1 中写入 k1, v1
        let handle = thread::spawn(move || {
            let res = cloned.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            assert_res_ok(res, &[Value::default()], &[]);
        });
        handle.join().unwrap();

        // 在当前线程下读取 table t1 的 k1，应该返回 v1
        let res = service.execute(CommandRequest::new_hget("t1", "k1"));
        assert_res_ok(res, &["v1".into()], &[]);
    }

    #[test]
    fn event_registration_should_work() {
        fn b(cmd: &CommandRequest) -> Option<CommandResponse> {
            info!("Got {:?}", cmd);
            None
        }
        fn c(res: &CommandResponse) -> Option<CommandResponse> {
            info!("{:?}", res);
            None
        }
        fn d(res: &mut CommandResponse) -> Option<CommandResponse> {
            res.status = StatusCode::CREATED.as_u16() as _;
            None
        }
        fn e() -> Option<CommandResponse> {
            info!("Data is sent");
            None
        }

        let service: Service = Service::new(MemTable::default())
            .fn_received(|_: &CommandRequest| None)
            .fn_received(b)
            .fn_executed(c)
            .fn_before_send(d)
            .fn_after_send(e);

        let res = service.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
        assert_eq!(res.status, StatusCode::CREATED.as_u16() as _);
        assert_eq!(res.message, "");
        assert_eq!(res.values, vec![Value::default()]);
    }

    #[test]
    fn early_return_for_special_keys() {
        // 定义一个检查特殊键的回调
        fn check_special_keys(cmd: &CommandRequest) -> Option<CommandResponse> {
            match &cmd.request_data {
                Some(RequestData::Hget(param)) => {
                    if param.key == "admin" {
                        // 如果是查询 admin 键，直接返回权限错误
                        Some(
                            KvError::PermissionDenied("Cannot access admin key".to_string()).into(),
                        )
                    } else {
                        None
                    }
                }
                Some(RequestData::Hset(param)) => {
                    if let Some(pair) = &param.pair {
                        if pair.key == "admin" {
                            // 如果是设置 admin 键，直接返回权限错误
                            Some(
                                KvError::PermissionDenied("Cannot modify admin key".to_string())
                                    .into(),
                            )
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            }
        }

        let service = Service::new(MemTable::default()).fn_received(check_special_keys);

        // 尝试获取 admin 键
        let cmd = CommandRequest::new_hget("t1", "admin");
        let res = service.execute(cmd);
        assert_eq!(res.status, 403); // 应该返回权限错误
        assert!(res.message.contains("Cannot access admin key"));

        // 尝试设置 admin 键
        let cmd = CommandRequest::new_hset("t1", "admin", "secret".into());
        let res = service.execute(cmd);
        assert_eq!(res.status, 403); // 应该返回权限错误
        assert!(res.message.contains("Cannot modify admin key"));

        // 正常操作其他键
        let cmd = CommandRequest::new_hset("t1", "user1", "normal".into());
        let res = service.execute(cmd);
        assert_eq!(res.status, 200); // 正常处理
        assert_eq!(res.values, vec![Value::default()]);
    }
}

use crate::command_request::RequestData;
#[cfg(test)]
use crate::{Kvpair, Value};

// 测试成功返回的结果
#[cfg(test)]
pub fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[Kvpair]) {
    res.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.values, values);
    assert_eq!(res.pairs, pairs);
}

// 测试失败返回的结果
#[cfg(test)]
pub fn assert_res_error(res: CommandResponse, code: u32, msg: &str) {
    assert_eq!(res.status, code);
    assert!(res.message.contains(msg));
    assert_eq!(res.values, &[]);
    assert_eq!(res.pairs, &[]);
}
