use bytes::Bytes;
use std::{time::Duration, vec};

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
        let key = frames.next_string().unwrap();

        Ok(Get { key })
    }

    pub async fn apply(&self, conn: &mut Connection, db: &mut Db) -> crate::Result<()> {
        let response = match db.get(&self.key) {
            Some(bytes) => Frame::Bulk(bytes),
            None => Frame::Null,
        };

        println!("EXEC APPLY GET");

        conn.write_frame(&response).await?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct Set {
    key: String,
    value: Bytes,
    expire: Option<Duration>,
}

impl Set {
    fn parse_frames(frames: &mut vec::IntoIter<Frame>) -> crate::Result<Set> {
        let key = frames.next_string().unwrap();
        let value = frames.next_bytes()?;

        let mut expire = None;

        if let Some(_expire_type) = frames.next_string() {
            if let Ok(e) = frames.next_int() {
                expire = Some(Duration::from_millis(e));
            }
        }

        Ok(Set { key, value, expire })
    }

    pub async fn apply(&self, conn: &mut Connection, db: &mut Db) -> crate::Result<()> {
        db.set(&self.key, self.value.clone(), self.expire);

        let response = Frame::Simple("OK".to_string());

        println!("EXEC APPLY SET");

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

        let name = frame_iter.next_string().unwrap();

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
    fn next_string(&mut self) -> Option<String>;
    fn next_int(&mut self) -> crate::Result<u64>;
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

    fn next_string(&mut self) -> Option<String> {
        if let Some(frame) = self.next() {
            let string = match frame {
                Frame::Simple(s) => s,
                Frame::Bulk(data) => std::str::from_utf8(&data[..])
                    .map(|s| s.to_string())
                    .unwrap(),
                Frame::Integer(_) => todo!(),
                Frame::Array(_) => todo!(),
                Frame::Error(_) => todo!(),
                Frame::Null => todo!(),
            };

            Some(string)
        } else {
            None
        }
    }

    fn next_int(&mut self) -> crate::Result<u64> {
        let frame = self.next().unwrap();

        // let num = std::str::from_utf8(line).unwrap().parse::<u64>().unwrap();

        let int = match frame {
            Frame::Simple(s) => s.parse::<u64>().unwrap(),
            Frame::Bulk(data) => std::str::from_utf8(&data[..])
                .map(|s| s.parse::<u64>().unwrap())
                .unwrap(),
            Frame::Integer(int) => int,
            Frame::Array(_) => todo!(),
            Frame::Error(_) => todo!(),
            Frame::Null => todo!(),
        };

        Ok(int)
    }
}
