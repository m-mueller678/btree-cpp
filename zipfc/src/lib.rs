use rand::distributions::Uniform;
use rand::prelude::*;
use zipf::ZipfDistribution;

#[no_mangle]
pub unsafe extern "C" fn zipf_generate(mut num_elements: u32, zipf_parameter: f64, data_out: *mut u32, count: u32) {
    if num_elements == 0 {
        num_elements = 1;
    }
    let generator = if zipf_parameter > 0.0 {
        Ok(ZipfDistribution::new(num_elements as usize, zipf_parameter).unwrap())
    } else if zipf_parameter < 0.0 {
        Err(Uniform::new(0, num_elements))
    } else {
        panic!("invalid zipf parameter {zipf_parameter}")
    };
    let mut rng = SmallRng::from_entropy();
    for i in 0..count as usize {
        data_out.add(i).write(match generator {
            Ok(zipf) => zipf.sample(&mut rng) as u32 - 1,
            Err(uniform) => uniform.sample(&mut rng),
        });
    }
}