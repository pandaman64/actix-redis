use actix::Message;
use redis_async::{error::Error as RedisError, resp::RespValue};

use Error;

pub trait Command {
    type Output;

    fn into_request(self) -> RespValue;
    fn from_response(res: RespValue) -> Result<Self::Output, RedisError>;
}

#[derive(Debug)]
pub struct Get {
    pub key: String,
}

impl Message for Get {
    type Result = Result<Option<Vec<u8>>, Error>;
}

impl Command for Get {
    type Output = Option<Vec<u8>>;

    fn into_request(self) -> RespValue {
        resp_array!["GET", self.key]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RedisError> {
        match res {
            RespValue::BulkString(s) => Ok(Some(s)),
            RespValue::Nil => Ok(None),
            _ => Err(RedisError::RESP(
                "invalid response for GET".into(),
                Some(res),
            )),
        }
    }
}

#[derive(Debug)]
pub enum Expiration {
    Infinite,
    Ex(String),
    Px(String),
}

#[derive(Debug)]
pub struct Set {
    pub key: String,
    pub value: String,
    pub expiration: Expiration,
}

impl Message for Set {
    type Result = Result<(), Error>;
}

impl Command for Set {
    type Output = ();

    fn into_request(self) -> RespValue {
        use self::Expiration::*;

        match self.expiration {
            Infinite => resp_array!["SET", self.key, self.value],
            Ex(ex) => resp_array!["SET", self.key, self.value, "EX", ex],
            Px(px) => resp_array!["SET", self.key, self.value, "PX", px],
        }
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RedisError> {
        // TODO: SET with NX/XX can return Null reply
        match res {
            RespValue::SimpleString(ref s) if s == "OK" => Ok(()),
            _ => Err(RedisError::RESP(
                "invalid response for SET".into(),
                Some(res),
            )),
        }
    }
}

#[derive(Debug)]
pub struct Expire {
    pub key: String,
    pub seconds: String,
}

impl Message for Expire {
    type Result = Result<bool, Error>;
}

impl Command for Expire {
    /// true if the timeout was set, false if key does not exist
    type Output = bool;

    fn into_request(self) -> RespValue {
        resp_array!["EXPIRE", self.key, self.seconds]
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RedisError> {
        match res {
            RespValue::Integer(1) => Ok(true),
            RespValue::Integer(0) => Ok(false),
            _ => Err(RedisError::RESP(
                "invalid response for EXPIRE".into(),
                Some(res),
            )),
        }
    }
}

#[derive(Debug)]
pub struct Del {
    pub keys: Vec<String>,
}

impl Message for Del {
    type Result = Result<i64, Error>;
}

impl Command for Del {
    /// the number of keys that were removed
    type Output = i64;

    fn into_request(self) -> RespValue {
        let mut v = vec![RespValue::BulkString(b"EXPIRE".to_vec())];
        v.extend(self.keys.into_iter().map(Into::into));
        RespValue::Array(v)
    }

    fn from_response(res: RespValue) -> Result<Self::Output, RedisError> {
        match res {
            RespValue::Integer(num) => Ok(num),
            _ => Err(RedisError::RESP(
                "invalid response for DEL".into(),
                Some(res),
            )),
        }
    }
}
