fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/hyperliquid_bbo.proto");

    tonic_build::configure()
        .build_server(false)
        .compile(&["proto/hyperliquid_bbo.proto"], &["proto"])?;

    Ok(())
}
