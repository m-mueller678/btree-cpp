use rand::distributions::Uniform;
use rand::prelude::*;
use zipf::ZipfDistribution;

pub struct Generator {
    generator: Result<ZipfDistribution, Uniform<u32>>,
    rng: SmallRng,
}

#[no_mangle]
pub unsafe extern "C" fn zipf_init_generator(num_elements: u32, zipf_parameter: f64) -> *mut Generator {
    Box::into_raw(
        Box::new(
            Generator {
                generator: if zipf_parameter > 0.0 {
                    Ok(ZipfDistribution::new(num_elements as usize, zipf_parameter).unwrap())
                } else if zipf_parameter < 0.0 {
                    Err(Uniform::new(0, num_elements))
                } else {
                    panic!("invalid zipf parameter {zipf_parameter}")
                },
                rng: SmallRng::from_entropy(),
            }
        )
    )
}

#[no_mangle]
pub unsafe extern "C" fn zipf_generate(this: *mut Generator, data_out: *mut u32, count: u32) {
    let this = &mut *this;
    for i in 0..count as usize {
        data_out.add(i).write(match this.generator {
            Ok(zipf) => zipf.sample(&mut this.rng) as u32 - 1,
            Err(uniform) => uniform.sample(&mut this.rng),
        });
    }
}