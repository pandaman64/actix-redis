use std::collections::HashMap;

use super::{Command, Error, RedisActor, RespValue};

use actix::prelude::*;
use futures::prelude::*;

const MAX_RETRY: usize = 16;

struct Moved {
    command: RespValue,
    redirect_to: String,
    retry: usize,
}

impl Message for Moved {
    type Result = Result<RespValue, Error>;
}

pub struct RedisClusterActor {
    initial_nodes: Vec<String>,
    nodes: HashMap<String, Addr<RedisActor>>,
}

impl RedisClusterActor {
    pub fn start<S: IntoIterator<Item = String>>(addrs: S) -> Addr<RedisClusterActor> {
        let initial_nodes = addrs.into_iter().collect();
        Supervisor::start(|_| RedisClusterActor {
            initial_nodes,
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
    }
}

impl Supervised for RedisClusterActor {
    fn restarting(&mut self, _: &mut Self::Context) {
        // TODO: what to do
    }
}

fn handle_response(
    command: RespValue, retry: usize, res: Result<RespValue, Error>,
    actor: &mut RedisClusterActor, ctx: &mut Context<RedisClusterActor>,
) -> Box<ActorFuture<Item = RespValue, Error = Error, Actor = RedisClusterActor>> {
    match res {
        Ok(RespValue::Error(ref err))
            if err.starts_with("MOVED") && retry < MAX_RETRY =>
        {
            info!(
                "MOVED redirection: retry = {}, command = {:?}",
                retry, command
            );

            let mut values = err.split(' ');
            let _moved = values.next().unwrap();
            // TODO: add slot to slot table
            let _slot = values.next().unwrap();
            let node = values.next().unwrap();

            Box::new(
                ctx.address()
                    .send(Moved {
                        command,
                        redirect_to: node.into(),
                        retry,
                    })
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
        match self.nodes.get(&moved.redirect_to) {
            Some(node) => Box::new(
                node.send(Command(moved.command.clone()))
                    .map_err(|_| Error::Disconnected)
                    .into_actor(self)
                    .and_then(|res, actor, ctx| {
                        handle_response(moved.command, moved.retry + 1, res, actor, ctx)
                    }),
            ),
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
                .and_then(|res, actor, ctx| handle_response(msg.0, 0, res, actor, ctx)),
        )
    }
}
