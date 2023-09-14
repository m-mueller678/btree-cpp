use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use rand::distributions::Uniform;
use rand::prelude::*;
use zipf::ZipfDistribution;

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