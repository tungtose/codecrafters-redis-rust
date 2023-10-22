use bytes::Bytes;
use std::vec;

use crate::{db::Db, frame::Frame, Connection};

#[derive(Debug)]
pub enum Command {
    Ping(Ping),
    Echo(Echo),
    Set(Set),
    Get(Get),
}

#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    fn parse_frames(frames: &mut vec::IntoIter<Frame>) -> crate::Result<Get> {
        let key = frames.next_string()?;

        Ok(Get { key })
    }

    pub async fn apply(&self, conn: &mut Connection, db: &mut Db) -> crate::Result<()> {
        let response = match db.get(&self.key) {
            Some(bytes) => Frame::Bulk(bytes),
            None => Frame::Null,
        };

        conn.write_frame(&response).await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct Set {
    key: String,
    value: Bytes,
}

impl Set {
    fn parse_frames(frames: &mut vec::IntoIter<Frame>) -> crate::Result<Set> {
        let key = frames.next_string()?;
        let value = frames.next_bytes()?;

        Ok(Set { key, value })
    }

    pub async fn apply(&self, conn: &mut Connection, db: &mut Db) -> crate::Result<()> {
        db.set(&self.key, self.value.clone());

        let response = Frame::Simple("OK".to_string());

        conn.write_frame(&response).await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct Ping;

impl Ping {
    pub async fn apply(&self, conn: &mut Connection) -> crate::Result<()> {
        let response = Frame::Simple("PONG".to_string());

        conn.write_frame(&response).await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct Echo {
    msg: Bytes,
}

impl Echo {
    fn parse_frames(frames: &mut vec::IntoIter<Frame>) -> crate::Result<Echo> {
        let bytes = frames.next_bytes()?;

        Ok(Echo { msg: bytes })
    }

    pub async fn apply(&self, conn: &mut Connection) -> crate::Result<()> {
        let response = Frame::Bulk(self.msg.clone());

        conn.write_frame(&response).await?;

        Ok(())
    }
}

impl Command {
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        let mut frame_iter = match frame {
            Frame::Array(array) => array.into_iter(),
            _ => unreachable!(),
        };

        let name = frame_iter.next_string()?;

        let command = match &name[..] {
            "ping" => Command::Ping(Ping),
            "echo" => Command::Echo(Echo::parse_frames(&mut frame_iter)?),
            "set" => Command::Set(Set::parse_frames(&mut frame_iter)?),
            "get" => Command::Get(Get::parse_frames(&mut frame_iter)?),
            _ => unreachable!(),
        };

        Ok(command)
    }

    pub async fn apply(&self, conn: &mut Connection, db: &mut Db) -> crate::Result<()> {
        match self {
            Command::Ping(ping) => ping.apply(conn).await,
            Command::Echo(echo) => echo.apply(conn).await,
            Command::Set(set) => set.apply(conn, db).await,
            Command::Get(get) => get.apply(conn, db).await,
        }
    }
}

pub trait FrameIter {
    fn next_bytes(&mut self) -> crate::Result<Bytes>;
    fn next_string(&mut self) -> crate::Result<String>;
}

impl FrameIter for std::vec::IntoIter<Frame> {
    fn next_bytes(&mut self) -> crate::Result<Bytes> {
        let bytes = match self.next().unwrap() {
            Frame::Simple(s) => Bytes::from(s.into_bytes()),
            Frame::Bulk(data) => data,
            Frame::Integer(_) => todo!(),
            Frame::Array(_) => todo!(),
            Frame::Error(_) => todo!(),
            Frame::Null => todo!(),
        };

        Ok(bytes)
    }

    fn next_string(&mut self) -> crate::Result<String> {
        let frame = self.next().unwrap();

        let string = match frame {
            Frame::Simple(s) => s,
            Frame::Bulk(data) => std::str::from_utf8(&data[..])
                .map(|s| s.to_string())
                // .map_err(|_| "invalid string".into())
                .unwrap(),
            Frame::Integer(_) => todo!(),
            Frame::Array(_) => todo!(),
            Frame::Error(_) => todo!(),
            Frame::Null => todo!(),
        };

        Ok(string)
    }
}
