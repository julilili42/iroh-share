use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    iroh_share::desktop_main().await
}
