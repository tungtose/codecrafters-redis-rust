use std::{fmt, io::Cursor, num::TryFromIntError, string::FromUtf8Error};

use bytes::{Buf, Bytes};

pub enum Frame {
    Simple(String),
    Bulk(Bytes),
    Integer(u64),
    Array(Vec<Frame>),
    Error(String),
}

#[derive(Debug)]
pub enum Error {
    Incomplete,
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl Frame {
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        let byte = src.get_u8();

        println!("xxx: {:?}", std::str::from_utf8(&[byte]));
        match byte {
            b'*' => {
                println!("Got Array");
                // let len = Connection::get_line(&mut buf)?;
                let len = get_number(src)? as usize;
                println!("Len: {}", len);

                let mut out = Vec::with_capacity(len);

                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(out))
            }

            b'+' => {
                println!("Got simple");
                todo!()
            }

            b'$' => {
                println!("Got bulk");

                let len = get_number(src)? as usize;

                let data = Bytes::copy_from_slice(&src.chunk()[..len]);

                src.advance(len + 2);

                Ok(Frame::Bulk(data))
                // todo!()
            }

            b'-' => {
                println!("Got Error");
                todo!()
            }

            b':' => {
                println!("Got Integer");
                todo!()
            }
            _ => unimplemented!(),
        }
        // Ok(())
    }
}

pub fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
            let line = &src.get_ref()[start..i];
            println!("Line: {:?}", line);
            return Ok(line);
        }
    }

    unreachable!()
}

fn get_number(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    let line = get_line(src)?;
    let num = std::str::from_utf8(line).unwrap().parse::<u64>().unwrap();
    Ok(num)
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        use std::str;

        match self {
            Frame::Simple(response) => response.fmt(fmt),
            Frame::Error(msg) => write!(fmt, "error: {}", msg),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Bulk(msg) => match str::from_utf8(msg) {
                Ok(string) => string.fmt(fmt),
                Err(_) => write!(fmt, "{:?}", msg),
            },
            // Frame::Null => "(nil)".fmt(fmt),
            Frame::Array(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        // use space as the array element display separator
                        write!(fmt, " ")?;
                    }

                    part.fmt(fmt)?;
                }

                Ok(())
            }
        }
    }
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_src: FromUtf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_src: TryFromIntError) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}
