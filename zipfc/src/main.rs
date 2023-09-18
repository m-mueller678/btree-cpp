use zipfc::zipf_generate;

fn main(){
    let key_count:u32=std::env::var("K").unwrap().parse().unwrap();
    let sample_count:usize=std::env::var("S").unwrap().parse().unwrap();
    let zipf:f64=std::env::var("Z").unwrap().parse().unwrap();
    let mut out = vec![0u32;sample_count];
    unsafe{
        zipf_generate(key_count,zipf,out.as_mut_ptr(),out.len() as u32,false);
    }
    for x in &out {
        println!("{x}")
    }
}