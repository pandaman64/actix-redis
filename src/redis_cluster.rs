use std::collections::HashMap;

use super::{Command, Error, RedisActor, RespValue};

use actix::prelude::*;
use futures::prelude::*;

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

impl Handler<Command> for RedisClusterActor {
    type Result = ResponseActFuture<Self, RespValue, Error>;

    // TODO: loop
    fn handle(&mut self, msg: Command, _: &mut Self::Context) -> Self::Result {
        use actix::fut::ActorFuture;
        let node = self.pick_random_node().clone();

        Box::new(
            node.send(msg.clone())
                .map_err(|_| Error::Disconnected)
                .into_actor(self)
                .and_then(
                    |res,
                     this,
                     _ctx|
                     -> Box<
                        ActorFuture<Item = RespValue, Error = Error, Actor = Self>,
                    > {
                        match res {
                            Ok(RespValue::Error(ref err))
                                if err.starts_with("MOVED") =>
                            {
                                let mut values = err.split(' ');
                                let _moved = values.next().unwrap();
                                let _slot = values.next().unwrap();
                                let node = values.next().unwrap();

                                match this.nodes.get(node) {
                                    Some(node) => Box::new(
                                        node.send(msg)
                                            .map_err(|_| Error::Disconnected)
                                            .and_then(|res| res)
                                            .into_actor(this),
                                    ),
                                    None => unimplemented!(),
                                }
                            }
                            _ => Box::new(res.into_future().into_actor(this)),
                        }
                    },
                ),
        )

        /*
        use futures::future::loop_fn;
        use futures::future::Loop;
        Box::new(loop_fn(node.clone(),move |node| {
            node.send(msg)
                .map_err(|_| Error::Disconnected)
                .map(|res| match res {
                    Ok(RespValue::Error(ref err)) if err.starts_with("MOVED") => {
                        let values = err.split(' ');
                        let _moved = values.next().unwrap();
                        let _slot = values.next().unwrap();
                        let node = values.next().unwrap();
        
                        match self.nodes.get(node) {
                            Some(node) => Loop::Continue(node.clone()),
                            None => unimplemented!(),
                        }
                    },
                    _ => Loop::Break(res),
                })
        }).and_then(|res| res).into_actor(self))
        */
    }
}
