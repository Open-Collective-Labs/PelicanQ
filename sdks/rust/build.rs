fn main() {
    let manifest_dir = std::path::PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR").unwrap(),
    );
    let proto_root = manifest_dir.join("../../proto");

    tonic_build::configure()
        .compile(
            &[
                "pelicanq/v1/message.proto",
                "pelicanq/v1/queue.proto",
                "pelicanq/v1/admin.proto",
            ],
            &[&proto_root],
        )
        .expect("failed to compile protos");
}
