use bytes::BytesMut;
use std::{
    error::Error,
    io::{self, Cursor},
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (socket, _) = listener.accept().await?;

        process_socket(socket).await?;
    }
}

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

enum Frame {
    Simple(String),
    Error(String),
}

struct Connection {
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

    pub fn read_frame(&self) -> Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.buffer[..]);

        todo!();
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
            Frame::Error(_) => todo!(),
        }

        Ok(())
    }
}

async fn process_socket(socket: TcpStream) -> io::Result<()> {
    // println!("Socket: {:?}", socket);

    let mut connection = Connection::new(socket);

    let pong_frame = Frame::Simple("PONG".to_string());

    connection.write_frame(&pong_frame).await?;

    Ok(())
}
