use std::vec;

use bytes::Bytes;

use crate::{frame::Frame, Connection};

#[derive(Debug)]
pub enum Command {
    Ping(Ping),
    Echo(Echo),
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
        // TODO: fixme
        let bytes = match frames.next().unwrap() {
            Frame::Simple(s) => Bytes::from(s.into_bytes()),
            Frame::Bulk(data) => data,
            Frame::Integer(_) => todo!(),
            Frame::Array(_) => todo!(),
            Frame::Error(_) => todo!(),
        };

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

        let frame = frame_iter.next().unwrap();

        let name = match frame {
            Frame::Simple(s) => s,
            Frame::Bulk(data) => std::str::from_utf8(&data[..])
                .map(|s| s.to_string())
                // .map_err(|_| "invalid string".into())
                .unwrap(),
            Frame::Integer(_) => todo!(),
            Frame::Array(_) => todo!(),
            Frame::Error(_) => todo!(),
        };

        let command = match &name[..] {
            "ping" => Command::Ping(Ping),
            "echo" => Command::Echo(Echo::parse_frames(&mut frame_iter)?),
            _ => unreachable!(),
        };

        Ok(command)
    }

    pub async fn apply(&self, conn: &mut Connection) -> crate::Result<()> {
        match self {
            Command::Ping(ping) => ping.apply(conn).await,
            Command::Echo(echo) => echo.apply(conn).await,
        }
    }
}
