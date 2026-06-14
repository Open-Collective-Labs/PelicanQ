fn main() {
    tonic_build::configure()
        .compile(
            &[
                "pelicanq/v1/message.proto",
                "pelicanq/v1/queue.proto",
                "pelicanq/v1/admin.proto",
            ],
            &["../proto"],
        )
        .expect("failed to compile protos");
}
