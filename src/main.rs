use bytes::BytesMut;
use std::{
    error::Error,
    io::{self, Cursor},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                tokio::spawn(async move {
                    process_socket(socket).await;
                });
            }
            Err(err) => {
                println!("Error: {:?}", err);
            }
        }
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

    pub async fn get_line(&mut self) -> String {
        let (reader, _) = self.stream.split();
        let mut buf_read = BufReader::new(reader);

        let mut buf = String::new();

        let _line = buf_read.read_line(&mut buf).await;

        buf

        // match line {
        //     Ok(line) => buf,
        //     Err(_e) => String::new(),
        // }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        self.write_value(frame).await?;

        self.stream.flush().await
    }

    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                println!("write: {}", val);
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(_) => todo!(),
        }

        Ok(())
    }
}

async fn process_socket(mut socket: TcpStream) -> anyhow::Result<()> {
    let (reader, mut writer) = socket.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    while reader.read_line(&mut line).await? > 0 {
        if line.to_ascii_uppercase().starts_with("PING") {
            writer.write_all(b"+PONG\r\n").await?;
        }
        line.clear();
    }
    // Ok(()) // println!("Socket: {:?}", socket);

    // let mut connection = Connection::new(socket);

    // let mut line = connection.get_line().await;

    // println!("Line: {}", line);

    // while line.len() > 0 {
    //     let simple_frame = Frame::Simple("PONG".to_string());
    //     connection.write_frame(&simple_frame).await?;
    //     line.clear()
    // }
    Ok(())
}
