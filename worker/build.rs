fn main() {
    prost_build::compile_protos(&["../node/proto/transaction.proto"], &["../node/proto/"])
        .unwrap();
}

