server:
	RUST_LOG=info cargo run --example server_with_sled --quiet

client:
	RUST_LOG=info cargo run --example client --quiet

build:
	cargo build

.PHONY: server client build