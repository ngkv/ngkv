use std::cmp;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct BloomFilter {
    k: u32,
    buf: Vec<u8>,
}

unsafe fn read_u32_le_unchecked(ptr: *const u8) -> u32 {
    let mut bytes = [0u8; 4];
    std::ptr::copy_nonoverlapping(ptr, bytes.as_mut_ptr(), 4);
    u32::from_le_bytes(bytes)
}

fn hash(data: &[u8], seed: u32) -> u32 {
    let m = 0xc6a4a793u32;
    let r = 24u32;
    let n = data.len() as u32;

    let mut h = seed ^ n.wrapping_mul(m);
    unsafe {
        let mut cur = data.as_ptr();
        let limit = cur.add(data.len());
        while cur.add(4) <= limit {
            let w = read_u32_le_unchecked(cur);
            cur = cur.add(4);
            h = h.wrapping_add(w);
            h = h.wrapping_mul(m);
            h ^= h >> 16;
        }

        let rem = limit.offset_from(cur);
        if rem >= 3 {
            h = h.wrapping_add((*cur.add(2) as u32) << 16);
        }
        if rem >= 2 {
            h = h.wrapping_add((*cur.add(1) as u32) << 8);
        }
        if rem >= 1 {
            h = h.wrapping_add(*cur as u32);
            h = h.wrapping_mul(m);
            h ^= h >> r;
        }
        h
    }
}

fn bloom_hash(key: &[u8]) -> u32 {
    hash(key, 0xbc9f1d3)
}

impl BloomFilter {
    pub fn likely_contains(&self, key: &[u8]) -> bool {
        let bits = self.buf.len() as u32 * 8;
        let mut h = bloom_hash(key);
        let delta = (h >> 17) | (h << 15);
        for _j in 0..self.k {
            let bit_pos = h % bits;
            if self.buf[bit_pos as usize / 8] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }

        true
    }
}

pub struct BloomFilterBuilder {
    k: u32,
    bits_per_key: u32,
    hashes: Vec<u32>,
}

impl BloomFilterBuilder {
    pub fn new(bits_per_key: u32) -> Self {
        let mut k = (bits_per_key as f32 * 0.69f32) as u32;
        k = num::clamp(k, 1, 30);

        Self {
            k,
            bits_per_key,
            hashes: vec![],
        }
    }

    pub fn add_key(&mut self, key: &[u8]) -> &mut Self {
        let h = bloom_hash(key);
        self.hashes.push(h);
        self
    }

    pub fn build(&self) -> BloomFilter {
        let mut bits = self.bits_per_key * self.hashes.len() as u32;
        bits = cmp::max(bits, 64);

        let bytes = num::integer::div_ceil(bits, 8);
        bits = bytes * 8;

        let mut buf = vec![0u8; bytes as usize];

        for &h in self.hashes.iter() {
            let mut h = h;
            let delta = (h >> 17) | (h << 15);
            for _j in 0..self.k {
                let bit_pos = h % bits;
                buf[bit_pos as usize / 8] |= 1 << (bit_pos % 8);
                h = h.wrapping_add(delta);
            }
        }

        BloomFilter { k: self.k, buf }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use itertools::Itertools;
    use rand::{distributions::Uniform, prelude::*};

    #[test]
    fn bloom_simple_contains() {
        let mut builder = BloomFilterBuilder::new(5);
        builder.add_key("test11".as_bytes());
        builder.add_key("test12".as_bytes());
        let bloom = builder.build();
        assert!(bloom.likely_contains("test11".as_bytes()));
        assert!(bloom.likely_contains("test12".as_bytes()));
    }

    fn random_strings(cnt: u32) -> Vec<String> {
        let mut res_set = HashSet::new();
        let mut rng = StdRng::seed_from_u64(0);
        while res_set.len() < cnt as usize {
            let len = rng.sample(Uniform::new(1, 100));
            let mut s = String::new();
            for _ in 0..len {
                s.push(rng.sample(Uniform::new_inclusive('a', 'z')));
            }
            res_set.insert(s);
        }

        res_set.into_iter().collect_vec()
    }

    #[test]
    fn bloom_lot_contains() {
        let strs = random_strings(1000);
        let mut builder = BloomFilterBuilder::new(5);
        for s in strs.iter() {
            builder.add_key(s.as_bytes());
        }

        let bloom = builder.build();
        for s in strs.iter() {
            assert!(bloom.likely_contains(s.as_bytes()));
        }
    }

    /// Test the false positive rate is not too high.
    #[test]
    fn bloom_fpr_test() {
        // Bloom filter config:
        // n=1000, bits_per_key=5, k=5*0.69=3
        // So we have p<0.1.
        //
        // https://hur.st/bloomfilter/?n=1000&p=&m=5000&k=3

        let n_out_filter = 2000;

        let strs = random_strings(1000 + n_out_filter);
        let (in_filter, out_filter) = strs.split_at(1000);
        let mut builder = BloomFilterBuilder::new(5);
        for k in in_filter {
            builder.add_key(k.as_bytes());
        }
        let bloom = builder.build();

        let mut fp = 0;
        for k in out_filter {
            if bloom.likely_contains(k.as_bytes()) {
                fp += 1;
            }
        }

        let fpr = fp as f32 / n_out_filter as f32;
        assert!(fpr < 0.1, "fpr is {}", fpr);
    }
}
