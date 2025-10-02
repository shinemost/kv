server-mem:
	RUST_LOG=info cargo run --example server --quiet

server-selddb:
	RUST_LOG=info cargo run --example server_with_selddb --quiet

server-rocksdb:
	RUST_LOG=info cargo run --example server_with_rocksdb --quiet

server-codec:
	RUST_LOG=info cargo run --example server_with_codec --quiet

kvc:
	RUST_LOG=INFO cargo run --bin kvc --quiet

kvs:
	RUST_LOG=INFO cargo run --bin kvs --quiet

kvnc:
	RUST_LOG=info cargo run --bin kvnc --quiet

kvns:
	RUST_LOG=info cargo run --bin kvns --quiet

build:
	RUSTFLAGS="-Clinker-plugin-lto -Clinker=clang -Clink-arg=-fuse-ld=lld" cargo build

bench:
	 RUST_LOG=info cargo bench

.PHONY: kvs kvc build kvnc kvns bench
