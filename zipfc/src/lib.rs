use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use rand::distributions::Uniform;
use rand::prelude::*;
use zipf::ZipfDistribution;
use bloomfilter::Bloom;

#[no_mangle]
pub unsafe extern "C" fn zipf_generate(mut num_elements: u32, zipf_parameter: f64, data_out: *mut u32, count: u32, shuffle: bool) {
    const SHUFFLE_SCALE: usize = 1 << 20;
    if num_elements == 0 {
        num_elements = 1;
    }
    let generator = if zipf_parameter > 0.0 {
        Ok(ZipfDistribution::new(num_elements as usize * if shuffle { SHUFFLE_SCALE } else { 1 }, zipf_parameter).unwrap())
    } else {
        Err(Uniform::new(0, num_elements))
    };
    let mut rng = SmallRng::from_entropy();
    for i in 0..count as usize {
        data_out.add(i).write(match generator {
            Ok(zipf) => {
                if shuffle {
                    // this modulo zipf sampling is described in the ycsb paper
                    let mut hasher = DefaultHasher::default();
                    hasher.write_usize(zipf.sample(&mut rng));
                    let x = (hasher.finish() % num_elements as u64) as u32;
                    x
                } else {
                    zipf.sample(&mut rng) as u32
                }
            }
            Err(uniform) => uniform.sample(&mut rng),
        });
    }
}

#[no_mangle]
pub unsafe extern "C" fn generate_rng4(seed: u64, num_elements: u32, out: *mut u32) {
    let num_elements = num_elements as usize;
    let mut rng = SmallRng::seed_from_u64(seed);
    //let mut hash_seed = [0u8;32];
    //rng.fill_bytes(&mut has_seed);
    let mut bloom = Bloom::new_for_fp_rate_with_seed(num_elements, 0.01, &rng.gen());
    for i in 0..num_elements {
        let next = loop {
            let candidate: u32 = rng.gen();
            if !bloom.check(&candidate) {
                break candidate;
            }
        };
        bloom.set(&next);
        out.add(i).write(next);
    }
}


#[no_mangle]
pub unsafe extern "C" fn generate_rng8(seed: u64, num_elements: u32, out: *mut u64) {
    let num_elements = num_elements as usize;
    let mut rng = SmallRng::seed_from_u64(seed);
    //let mut hash_seed = [0u8;32];
    //rng.fill_bytes(&mut has_seed);
    let mut bloom = Bloom::new_for_fp_rate_with_seed(num_elements, 0.01, &rng.gen());
    for i in 0..num_elements {
        let next = loop {
            let candidate: u64 = rng.gen();
            if !bloom.check(&candidate) {
                break candidate;
            }
        };
        bloom.set(&next);
        out.add(i).write(next);
    }
}