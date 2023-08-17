use rand::prelude::*;
use zipf::ZipfDistribution;

pub struct Generator {
    dist: ZipfDistribution,
    rng: SmallRng,
}

#[no_mangle]
pub unsafe extern "C" fn zipf_init_generator(num_elements: u32, zipf_paramter: f64) -> *mut Generator {
    Box::into_raw(
        Box::new(
            Generator {
                dist: ZipfDistribution::new(num_elements as usize, zipf_paramter).unwrap(),
                rng: SmallRng::from_entropy(),
            }
        )
    )
}

#[no_mangle]
pub unsafe extern "C" fn zipf_generate(generator: *mut Generator, data_out: *mut u32, count: u32) {
    let generator = &mut *generator;
    for i in 0..count as usize {
        data_out.add(i).write(generator.dist.sample(&mut generator.rng) as u32);
    }
}