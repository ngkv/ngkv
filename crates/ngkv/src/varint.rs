use std::io::{self, Read, Write};

fn read_u8(mut r: impl Read) -> io::Result<u8> {
    let mut buf = [0 as u8; 1];
    r.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub trait VarintRead: Read {
    fn read_var_u32(&mut self) -> io::Result<u32>;
}

impl<T: Read> VarintRead for T {
    fn read_var_u32(&mut self) -> io::Result<u32> {
        let mut res = 0u32;
        let mut offset = 0u32;
        loop {
            let cur = read_u8(&mut *self)?;
            res |= (cur as u32 & 0x7F) << offset;
            offset += 7;
            if cur & 0x80 == 0 {
                break;
            }
        }

        Ok(res)
    }
}

pub trait VarintWrite: Write {
    fn write_var_u32(&mut self, num: u32) -> io::Result<()>;
}

impl<T: Write> VarintWrite for T {
    fn write_var_u32(&mut self, num: u32) -> io::Result<()> {
        let mut buf = [0u8; 5];
        let mut written = 0;

        let mut rem = num;
        loop {
            let mut cur = (rem & 0x7F) as u8;
            rem >>= 7;
            if rem > 0 {
                cur = cur | 0x80;
            }
            buf[written] = cur;
            written += 1;
            if rem == 0 {
                break;
            }
        }

        let out = &mut buf[0..written];
        self.write_all(&out)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::{distributions::Uniform, prelude::*};

    use super::*;
    use std::io::Cursor;

    fn test_u32_iter(rand_cnt: usize) -> impl Iterator<Item = u32> {
        let special = vec![0, 60, 127, 130, 16383, 17000, u32::MAX];
        let rng = StdRng::seed_from_u64(0);
        special.into_iter().chain(
            rng.sample_iter(Uniform::new_inclusive(u32::MIN, u32::MAX))
                .take(rand_cnt),
        )
    }

    fn check_varint_size(num: u32, size: u64) {
        let mut buf = vec![0u8; 0];
        let mut cursor = Cursor::new(&mut buf);
        cursor.write_var_u32(num).unwrap();
        let written = cursor.position();
        assert!(written <= size);
    }

    #[test]
    fn varint_size() {
        check_varint_size(0, 1);
        check_varint_size(100, 1);
        check_varint_size(200, 2);
        check_varint_size(16383, 2);
        check_varint_size(17000, 3);
        check_varint_size(u32::MAX, 5);
    }

    #[test]
    fn varint_single_write_read_eq() {
        for num in test_u32_iter(10000) {
            let mut buf = vec![0u8; 0];
            let mut cursor = Cursor::new(&mut buf);

            cursor.write_var_u32(num).unwrap();
            cursor.set_position(0);
            let read = cursor.read_var_u32().unwrap();
            assert_eq!(num, read);
        }
    }

    #[test]
    fn varint_multile_write_read_eq() {
        let mut rng = StdRng::seed_from_u64(0);
        let mut nums = test_u32_iter(1000).collect_vec();
        nums.shuffle(&mut rng);

        let mut buf = vec![0u8; 0];
        let mut cursor = Cursor::new(&mut buf);
        for num in nums.iter().cloned() {
            cursor.write_var_u32(num).unwrap();
        }
        cursor.set_position(0);
        for num in nums.iter().cloned() {
            let read = cursor.read_var_u32().unwrap();
            assert_eq!(num, read);
        }
    }
}
