use std::io;
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (socket, _) = listener.accept().await?;

        process_socket(socket).await;
    }
}

async fn process_socket(mut socket: TcpStream) {
    println!("Socket: {:?}", socket);

    // socket.write_all(b"Hello").await;
}
