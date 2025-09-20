use crate::{command_request::RequestData, CommandRequest, CommandResponse, KvError, Storage};
use futures::stream;
use std::sync::Arc;
use tracing::debug;

mod command_service;
mod topic;
mod topic_service;

pub use topic::{Broadcaster, Topic};
pub use topic_service::{StreamingResponse, TopicService};

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
    broadcaster: Arc<Broadcaster>,
}

impl Clone for Service {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            on_received: self.on_received.clone(),
            on_executed: self.on_executed.clone(),
            on_before_send: self.on_before_send.clone(),
            on_after_send: self.on_after_send.clone(),
            broadcaster: Arc::clone(&self.broadcaster),
        }
    }
}

// Service 内部数据结构
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
            broadcaster: Default::default(),
        }
    }

    pub fn execute(&self, cmd: CommandRequest) -> StreamingResponse {
        debug!("Got request: {:?}", cmd);
        // self.store.deref()解引用 Arc<dyn Storage> -> &dyn Storage
        // self.store.as_ref()同理
        let mut res = dispatch(cmd.clone(), self.store.as_ref());
        debug!("Executed response: {:?}", res);

        if res == CommandResponse::default() {
            dispatch_stream(cmd, Arc::clone(&self.broadcaster))
        } else {
            debug!("Executed response returned: {:?}", res);
            self.on_executed.notify(&res);
            self.on_before_send.notify(&mut res);
            if !self.on_before_send.is_empty() {
                debug!("Modified response: {:?}", res);
            }
            Box::pin(stream::once(async { Arc::new(res) }))
        }
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
        Some(RequestData::Hmset(param)) => param.execute(store),
        Some(RequestData::Hdel(param)) => param.execute(store),
        Some(RequestData::Hmdel(param)) => param.execute(store),
        Some(RequestData::Hexist(param)) => param.execute(store),
        Some(RequestData::Hmexist(param)) => param.execute(store),
        None => KvError::InvalidCommand("Request has no data".into()).into(),
        // 处理不了的返回一个啥都不包括的 Response，这样后续可以用 dispatch_stream 处理
        _ => CommandResponse::default(),
    }
}

/// 从 Request 中得到 Response，目前处理所有 PUBLISH/SUBSCRIBE/UNSUBSCRIBE
pub fn dispatch_stream(cmd: CommandRequest, topic: impl Topic) -> StreamingResponse {
    match cmd.request_data {
        Some(RequestData::Publish(param)) => param.execute(topic),
        Some(RequestData::Subscribe(param)) => param.execute(topic),
        Some(RequestData::Unsubscribe(param)) => param.execute(topic),
        // 如果走到这里，就是代码逻辑的问题，直接 crash 出来
        _ => unreachable!(),
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

use crate::{Kvpair, Value};

// 测试成功返回的结果

pub fn assert_res_ok(res: &CommandResponse, values: &[Value], pairs: &[Kvpair]) {
    let mut sorted_pairs = res.pairs.clone();
    sorted_pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.values, values);
    assert_eq!(res.pairs, pairs);
}

// 测试失败返回的结果

pub fn assert_res_error(res: &CommandResponse, code: u32, msg: &str) {
    assert_eq!(res.status, code);
    assert!(res.message.contains(msg));
    assert_eq!(res.values, &[]);
    assert_eq!(res.pairs, &[]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemTable, Value};
    use http::StatusCode;
    use tokio_stream::StreamExt;
    use tracing::info;

    #[tokio::test]
    async fn service_should_works() {
        // 我们需要一个 service 结构至少包含 Storage
        let service = Service::new(MemTable::default());

        // service 可以运行在多线程环境下，它的 clone 应该是轻量级的
        let cloned = service.clone();

        // 创建一个线程，在 table t1 中写入 k1, v1
        tokio::spawn(async move {
            let mut res = cloned.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            let data = res.next().await.unwrap();
            assert_res_ok(&data, &[Value::default()], &[]);
        })
        .await
        .unwrap();

        // 在当前线程下读取 table t1 的 k1，应该返回 v1
        let mut res = service.execute(CommandRequest::new_hget("t1", "k1"));
        let data = res.next().await.unwrap();
        assert_res_ok(&data, &["v1".into()], &[]);
    }

    #[tokio::test]
    async fn event_registration_should_work() {
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

        let mut res = service.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
        let data = res.next().await.unwrap();
        assert_eq!(data.status, StatusCode::CREATED.as_u16() as _);
        assert_eq!(data.message, "");
        assert_eq!(data.values, vec![Value::default()]);
    }

    #[tokio::test]
    async fn early_return_for_special_keys() {
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
        let mut res = service.execute(cmd);
        let data = res.next().await.unwrap();
        assert_eq!(data.status, 404);

        // 尝试设置 admin 键
        let cmd = CommandRequest::new_hset("t1", "admin", "secret".into());
        let mut res = service.execute(cmd);
        let data = res.next().await.unwrap();
        assert_eq!(data.status, 200);
        assert_eq!(data.values, vec![Value::default()]);

        // 正常操作其他键
        let cmd = CommandRequest::new_hset("t1", "user1", "normal".into());
        let mut res = service.execute(cmd);
        let data = res.next().await.unwrap();
        assert_eq!(data.status, 200); // 正常处理
        assert_eq!(data.values, vec![Value::default()]);
    }
}
