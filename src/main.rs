mod commands;
mod db;
mod frame;

use bytes::BytesMut;
use db::Db;
use std::{
    error::Error,
    io::{self, Cursor},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use frame::Frame;

use commands::Command;

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let db = Db::new();

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                let mut db = db.clone();
                tokio::spawn(async move {
                    process_socket(socket, &mut db).await;
                });
            }
            Err(err) => {
                println!("Error: {:?}", err);
            }
        }
    }
}

pub struct Connection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(8192),
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        let read = self.stream.read_buf(&mut self.buffer).await?;

        println!("DBG: {:?}", self.buffer);

        if read == 0 {
            return Ok(None);
        }

        let mut buf = Cursor::new(&self.buffer[..]);

        let frame = Frame::parse(&mut buf)?;

        self.buffer.clear();

        println!("Frame: {}", frame);

        Ok(Some(frame))
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        self.write_value(frame).await?;

        self.stream.flush().await
    }

    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Bulk(data) => {
                let len = data.len();
                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(data).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Array(_) => todo!(),
            Frame::Error(_) => todo!(),
            Frame::Integer(_) => todo!(),
        }

        Ok(())
    }

    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        // Convert the value to a string
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}

async fn process_socket(socket: TcpStream, db: &mut Db) -> Result<()> {
    let mut connection = Connection::new(socket);

    loop {
        let frame = connection.read_frame().await?;

        match frame {
            Some(frame) => {
                let command = Command::from_frame(frame)?;

                command.apply(&mut connection, db).await?;
            }
            None => {
                break;
            }
        }
    }

    Ok(())
}
