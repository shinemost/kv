use snow::Builder;
use std::fs::File;
use std::io::Write;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    // 生成服务器密钥对
    let server_builder = Builder::new("Noise_XX_25519_ChaChaPoly_BLAKE2s".parse()?);
    let server_keypair = server_builder.generate_keypair()?;

    // 生成客户端密钥对
    let client_builder = Builder::new("Noise_XX_25519_ChaChaPoly_BLAKE2s".parse()?);
    let client_keypair = client_builder.generate_keypair()?;

    // 保存服务器私钥 (base64 编码)
    let mut server_private_file = File::create("fixtures_noise/server.key")?;
    server_private_file.write_all(&base64::encode(server_keypair.private).into_bytes())?;

    // 保存服务器公钥 (base64 编码)
    let mut server_pub_file = File::create("fixtures_noise/server.pub")?;
    server_pub_file.write_all(&base64::encode(server_keypair.public).into_bytes())?;

    // 保存客户端私钥 (base64 编码)
    let mut client_private_file = File::create("fixtures_noise/client.key")?;
    client_private_file.write_all(&base64::encode(client_keypair.private).into_bytes())?;

    // 保存客户端公钥 (base64 编码)
    let mut client_pub_file = File::create("fixtures_noise/client.pub")?;
    client_pub_file.write_all(&base64::encode(client_keypair.public).into_bytes())?;

    println!("密钥文件已生成到 fixtures_noise/ 目录:");
    println!("- server.key: 服务器私钥");
    println!("- server.pub: 服务器公钥");
    println!("- client.key: 客户端私钥");
    println!("- client.pub: 客户端公钥");

    Ok(())
}