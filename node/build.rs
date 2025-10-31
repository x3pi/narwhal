fn main() {
    prost_build::compile_protos(&["proto/comm.proto"], &["proto/"]).unwrap();
}
