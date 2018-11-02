use std::collections::HashMap;

use super::{Command, Error, RedisActor, RespValue};
use ::redis_async::resp::FromResp;
use ::redis_async::error::Error as RedisError;

use actix::prelude::*;
use futures::prelude::*;

struct Moved(RespValue, String);

impl Message for Moved {
    type Result = Result<RespValue, Error>;
}

struct Ask(RespValue, String);

impl Message for Ask {
    type Result = Result<RespValue, Error>;
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Slot {
    start: u16,
    end: u16,
    /// master node
    /// currently, post requests only to masters (not allowing read from stale slaves)
    master: (String, usize),
}

fn resp_to_node(res: RespValue) -> Result<(String, usize), RedisError> {
    if let RespValue::Array(node) = res {
        if node.len() < 2 {
            return Err(RedisError::RESP("A redis node must contain 2 elements".into(), Some(RespValue::Array(node))));
        }

        let mut iter = node.into_iter();
        let addr = String::from_resp(iter.next().unwrap())?;
        let port = usize::from_resp(iter.next().unwrap())?;

        Ok((addr, port))
    } else {
        Err(RedisError::RESP("A redis node must be array".into(), Some(res)))
    }
}

impl FromResp for Slot {
    fn from_resp_int(res: RespValue) -> Result<Self, RedisError> {
        if let RespValue::Array(slot) = res {
            if slot.len() < 3 {
                return Err(RedisError::RESP("A hash slot must contain at least 3 elements".into(), Some(RespValue::Array(slot))));
            }

            let mut iter = slot.into_iter();
            // TODO: handle out of range
            let start = i64::from_resp(iter.next().unwrap())? as u16;
            let end = i64::from_resp(iter.next().unwrap())? as u16;
            let master = resp_to_node(iter.next().unwrap())?;

            Ok(Slot {
                start,
                end,
                master,
            })
        } else {
            Err(RedisError::RESP("A hash slot must be an array".into(), Some(res)))
        }
    }
}

pub struct RedisClusterActor {
    initial_nodes: Vec<String>,
    nodes: HashMap<String, Addr<RedisActor>>,
    need_refresh_slots: bool,
    slots: Vec<Slot>,
}

impl RedisClusterActor {
    pub fn start(addrs: Vec<String>) -> Addr<RedisClusterActor> {
        Supervisor::start(|_| RedisClusterActor {
            initial_nodes: addrs,
            nodes: HashMap::new(),
            need_refresh_slots: true,
            slots: vec![],
        })
    }

    fn pick_random_node(&self) -> &Addr<RedisActor> {
        let mut rng = rand::thread_rng();
        match rand::seq::sample_iter(&mut rng, self.nodes.values(), 1) {
            Ok(v) => v[0],
            Err(_) => unimplemented!(),
        }
    }

    fn pick_node_by_key(&self, key: &[u8]) -> Option<&Addr<RedisActor>> {
        let key_slot = ::slot::hash_slot(key);

        // TODO: binary search slots
        for slot in self.slots.iter() {
            if slot.start <= key_slot && key_slot <= slot.end {
                // TODO: handle address and port in better way
                return self.nodes.get(&format!("{}:{}", slot.master.0, slot.master.1));
            }
        }

        None
    }
}

impl Actor for RedisClusterActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        for initial_node in self.initial_nodes.iter() {
            self.nodes.insert(
                initial_node.clone(),
                RedisActor::start(initial_node.clone()),
            );
        }

        let node = self.pick_random_node();
        ctx.spawn(
            node.send(Command(resp_array!["CLUSTER", "SLOTS"]))
                .into_actor(self)
                .and_then(|res, this, _ctx| {
                    debug!("cluster nodes: {:?}", res);
                    match res {
                        Ok(RespValue::Array(slots)) => {
                            this.slots = slots.into_iter().filter_map(|slot| Slot::from_resp(slot).ok()).collect();
                            info!("slots: {:?}", this.slots);
                            this.need_refresh_slots = false;
                        },
                        Err(_) => {},
                        _ => unimplemented!(),
                    };
                    Ok(()).into_future().into_actor(this)
                })
                .drop_err()
        );
    }
}

impl Supervised for RedisClusterActor {
    fn restarting(&mut self, _: &mut Self::Context) {
        // TODO: what to do
    }
}

fn handle_response(
    req: RespValue, res: Result<RespValue, Error>, actor: &mut RedisClusterActor,
    ctx: &mut Context<RedisClusterActor>,
) -> Box<ActorFuture<Item = RespValue, Error = Error, Actor = RedisClusterActor>> {
    match res {
        Ok(RespValue::Error(ref err)) if err.starts_with("MOVED") => {
            info!("MOVED redirection: {:?}", req);
            actor.need_refresh_slots = true;

            let mut values = err.split(' ');
            let _moved = values.next().unwrap();
            // TODO: add slot to slot table
            let _slot = values.next().unwrap();
            let node = values.next().unwrap();

            Box::new(
                ctx.address()
                    .send(Moved(req, node.into()))
                    .map_err(|_| Error::Disconnected)
                    .and_then(|res| res)
                    .into_actor(actor),
            )
        }
        Ok(RespValue::Error(ref err)) if err.starts_with("ASK") => {
            info!("ASK redirection: {:?}", req);
            let mut values = err.split(' ');
            let _ask = values.next().unwrap();
            let _slot = values.next().unwrap();
            let node = values.next().unwrap();

            Box::new(
                ctx.address()
                    .send(Ask(req, node.into()))
                    .map_err(|_| Error::Disconnected)
                    .and_then(|res| res)
                    .into_actor(actor),
            )
        }
        _ => Box::new(res.into_future().into_actor(actor)),
    }
}

impl Handler<Moved> for RedisClusterActor {
    type Result = ResponseActFuture<Self, RespValue, Error>;

    fn handle(&mut self, moved: Moved, _: &mut Self::Context) -> Self::Result {
        let command = moved.0;
        let node = moved.1;
        match self.nodes.get(&node) {
            Some(node) => Box::new(
                node.send(Command(command.clone()))
                    .map_err(|_| Error::Disconnected)
                    .into_actor(self)
                    .and_then(|res, actor, ctx| {
                        handle_response(command, res, actor, ctx)
                    }),
            ),
            None => unimplemented!(),
        }
    }
}

// TODO: test
impl Handler<Ask> for RedisClusterActor {
    type Result = ResponseActFuture<Self, RespValue, Error>;

    fn handle(&mut self, ask: Ask, _: &mut Self::Context) -> Self::Result {
        let command = ask.0;
        let node = ask.1;
        match self.nodes.get(&node) {
            Some(node) => {
                let node2 = node.clone();
                let command2 = command.clone();
                Box::new(
                    // TODO: other request might come between ASKING and the command
                    node.send(Command(resp_array!("ASKING")))
                        .map_err(|_| Error::Disconnected)
                        .and_then(move |_| {
                            node2
                                .send(Command(command))
                                .map_err(|_| Error::Disconnected)
                        })
                        .into_actor(self)
                        .and_then(|res, actor, ctx| {
                            handle_response(command2, res, actor, ctx)
                        }),
                )
            }
            None => unimplemented!(),
        }
    }
}

impl Handler<Command> for RedisClusterActor {
    type Result = ResponseActFuture<Self, RespValue, Error>;

    fn handle(&mut self, msg: Command, _: &mut Self::Context) -> Self::Result {
        use actix::fut::ActorFuture;
        let node = self.pick_random_node().clone();

        Box::new(
            node.send(msg.clone())
                .map_err(|_| Error::Disconnected)
                .into_actor(self)
                .and_then(|res, actor, ctx| handle_response(msg.0, res, actor, ctx)),
        )
    }
}
