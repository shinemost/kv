server:
	RUST_LOG=info cargo run --example server_with_sled --quiet

client:
	RUST_LOG=info cargo run --example client --quiet

build:
	RUSTFLAGS="-Clinker-plugin-lto -Clinker=clang -Clink-arg=-fuse-ld=lld" cargo build

.PHONY: server client build