use std::{io::{self, Read, Write}, u64};

// fn read_u8(mut r: impl Read) -> io::Result<u8> {
//     let mut buf = [0 as u8; 1];
//     r.read_exact(&mut buf)?;
//     Ok(buf[0])
// }

// pub trait VarintRead: Read {
//     fn read_var_u32(&mut self) -> io::Result<u32>;
// }

// impl<T: Read> VarintRead for T {
//     fn read_var_u32(&mut self) -> io::Result<u32> {
//         let mut res = 0u32;
//         loop {
//             let cur = read_u8(&mut *self)?;
//             res <<= 7;
//             res += (cur & 0x7F) as u32;
//             if cur | 0x80 == 0 {
//                 break;
//             }
//         }

//         Ok(res)
//     }
// }

// fn write_u8(mut w: impl Write, data: u8) -> io::Result<()> {
//     let buf = [data as u8; 1];
//     w.write_all(&buf)?;
//     Ok(())
// }

// pub trait VarintWrite: Write {
//     fn write_var_i32(&mut self) -> io::Result<()>;
// }

// impl<T: Write> VarintWrite for T {
//     fn write_var_i32(&mut self) -> io::Result<()> {
//         todo!()
//     }
// }

fn read_u8(buf: &mut &[u8]) -> u8 {
    let res = buf[0];
    *buf = &buf[1..];
    res
}

pub fn read_var_u32(buf: &mut &[u8]) -> u32 {
    let mut res = 0u32;
    loop {
        let cur = read_u8(buf);
        res <<= 7;
        res += (cur & 0x7F) as u32;
        if cur | 0x80 == 0 {
            break;
        }
    }

    res
}
