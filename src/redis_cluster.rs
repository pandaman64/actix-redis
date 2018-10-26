use std::collections::HashMap;

use super::{Command, Error, RedisActor, RespValue};

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

pub struct RedisClusterActor {
    initial_nodes: Vec<String>,
    nodes: HashMap<String, Addr<RedisActor>>,
}

impl RedisClusterActor {
    pub fn start(addrs: Vec<String>) -> Addr<RedisClusterActor> {
        Supervisor::start(|_| RedisClusterActor {
            initial_nodes: addrs,
            nodes: HashMap::new(),
        })
    }

    fn pick_random_node(&self) -> &Addr<RedisActor> {
        let mut rng = rand::thread_rng();
        match rand::seq::sample_iter(&mut rng, self.nodes.values(), 1) {
            Ok(v) => v[0],
            Err(_) => unimplemented!(),
        }
    }
}

impl Actor for RedisClusterActor {
    type Context = Context<Self>;

    fn started(&mut self, _: &mut Context<Self>) {
        for initial_node in self.initial_nodes.iter() {
            self.nodes.insert(
                initial_node.clone(),
                RedisActor::start(initial_node.clone()),
            );
        }

        // TODO: retrieve CLUSTER NODES
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
