extern crate actix;
extern crate actix_redis;
#[macro_use]
extern crate redis_async;
extern crate env_logger;
extern crate futures;

use actix::prelude::*;
use actix_redis::{Command, Error, RedisClusterActor, RespValue};
use futures::Future;

#[test]
fn test_error_connect() {
    let sys = System::new("test");

    let addr = RedisClusterActor::start(vec!["localhost:54000".into()]);
    let _addr2 = addr.clone();

    Arbiter::spawn_fn(move || {
        addr.send(Command(resp_array!["GET", "test"])).then(|res| {
            match res {
                Ok(Err(Error::NotConnected)) => (),
                _ => panic!("Should not happen {:?}", res),
            }
            System::current().stop();
            Ok(())
        })
    });

    sys.run();
}

#[test]
fn test_redis_cluster() {
    env_logger::init();
    let sys = System::new("test");

    let addr = RedisClusterActor::start(vec![
        "127.0.0.1:7000".into(),
        "127.0.0.1:7001".into(),
        "127.0.0.1:7002".into(),
    ]);
    let _addr2 = addr.clone();

    Arbiter::spawn_fn(move || {
        let addr2 = addr.clone();
        addr.send(Command(resp_array!["SET", "test", "value"]))
            .then(move |res| match res {
                Ok(Ok(resp)) => {
                    assert_eq!(resp, RespValue::SimpleString("OK".to_owned()));
                    addr2.send(Command(resp_array!["GET", "test"])).then(|res| {
                        match res {
                            Ok(Ok(resp)) => {
                                println!("RESP: {:?}", resp);
                                assert_eq!(
                                    resp,
                                    RespValue::BulkString((&b"value"[..]).into())
                                );
                            }
                            _ => panic!("Should not happen {:?}", res),
                        }
                        System::current().stop();
                        Ok(())
                    })
                }
                _ => panic!("Should not happen {:?}", res),
            })
    });

    sys.run();
}
