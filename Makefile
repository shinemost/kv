server-mem:
	RUST_LOG=info cargo run --example server --quiet

server-selddb:
	RUST_LOG=info cargo run --example server_with_selddb --quiet

server-rocksdb:
	RUST_LOG=info cargo run --example server_with_rocksdb --quiet

server-codec:
	RUST_LOG=info cargo run --example server_with_codec --quiet

client:
	RUST_LOG=info cargo run --bin kvc --quiet

server:
	RUST_LOG=info cargo run --bin kvs --quiet

build:
	RUSTFLAGS="-Clinker-plugin-lto -Clinker=clang -Clink-arg=-fuse-ld=lld" cargo build

.PHONY: server client build