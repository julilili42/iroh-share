use std::{
    env,
    path::{self, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use iroh::{EndpointAddr, endpoint_info::UserData};
use iroh_tickets::{Ticket, endpoint::EndpointTicket};
use tokio::{
    io::{self, AsyncBufReadExt, BufReader},
    sync::watch::Receiver,
    time,
};

use crate::protocol::Offer;

pub enum Command {
    Ui,
    Send {
        filename: String,
        endpoint_addr: Option<EndpointAddr>,
    },
    Receive {
        download_dir: PathBuf,
    },
    Help,
    Version,
}

pub fn parse_arguments(arg_refs: Vec<&str>) -> Result<Command> {
    match arg_refs.as_slice() {
        [] => Ok(Command::Ui),
        ["send", filename, ticket_str] => {
            let ticket = EndpointTicket::decode_string(ticket_str)
                .map_err(|e| anyhow!("failed to parse ticket: {}", e))?;
            let endpoint_addr = ticket.endpoint_addr().clone();

            Ok(Command::Send {
                filename: filename.to_string(),
                endpoint_addr: Some(endpoint_addr),
            })
        }
        ["send", filename] => Ok(Command::Send {
            filename: filename.to_string(),
            endpoint_addr: None,
        }),
        ["receive"] => Ok(Command::Receive {
            download_dir: env::current_dir()?,
        }),
        ["receive", download_dir] => Ok(Command::Receive {
            download_dir: path::absolute(download_dir)?,
        }),
        ["help"] | ["-h"] | ["--help"] => {
            print_usage();
            Ok(Command::Help)
        }
        ["version"] | ["-V"] | ["--version"] => {
            let name: &str = env!("CARGO_PKG_NAME");
            let version: &str = env!("CARGO_PKG_VERSION");
            println!("{name} v{version}");
            Ok(Command::Version)
        }
        ["send"] => anyhow::bail!("send requires <FILE> [TICKET]"),
        ["send", _, _, ..] => anyhow::bail!("send accepts <FILE> [TICKET]"),
        ["receive", _, ..] => anyhow::bail!("receive accepts [DOWNLOAD_DIR]"),
        [arg, ..] => {
            print_usage();
            anyhow::bail!("unknown command {arg}")
        }
    }
}

pub async fn select_receiver(
    mut rx: Receiver<Vec<(UserData, EndpointAddr)>>,
) -> Result<EndpointAddr> {
    let search_str = "Searching in local net...";
    println!("\n{}", search_str);
    rx.wait_for(|devices| !devices.is_empty())
        .await
        .context("device discovery stopped")?;

    time::sleep(Duration::from_secs(2)).await;

    let devices = rx.borrow().clone();

    let lines = devices
        .iter()
        .enumerate()
        .map(|(i, (user_data, _))| format!("{}. {}", i + 1, user_data));

    let title_str = "Receiver list.";
    let max_len = lines
        .clone()
        .map(|line| line.len())
        .max()
        .unwrap_or(0)
        .max(title_str.len())
        .max(search_str.len());

    println!("\n{}", title_str);
    println!("{}", "-".repeat(max_len));
    for line in lines {
        println!("{}", line);
    }
    println!("{}", "-".repeat(max_len));
    println!("\nSelect receiver:");
    let mut input = String::new();

    let mut stdin = BufReader::new(io::stdin());
    stdin.read_line(&mut input).await?;

    let idx = input
        .trim()
        .parse::<usize>()
        .context("selection must be a number")?;

    let (_, endpoint_addr) = idx
        .checked_sub(1)
        .and_then(|idx| devices.get(idx))
        .context("selection out of range")?;

    Ok(endpoint_addr.clone())
}

pub fn print_usage() {
    println!("Usage:");
    println!("    cargo run -- send <FILE> [TICKET]");
    println!("    cargo run -- receive [DOWNLOAD_DIR]");
    println!("    cargo run -- help");
    println!("    cargo run -- version");
}

pub async fn confirm(offer: &Offer) -> io::Result<bool> {
    println!(
        "{} ({} Bytes) accept? [y/n]",
        offer.filename, offer.filesize
    );

    let mut answer = String::new();
    BufReader::new(io::stdin()).read_line(&mut answer).await?;

    let decision = matches!(answer.trim().to_ascii_lowercase().as_str(), "y");
    Ok(decision)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::{Endpoint, endpoint::presets};

    #[test]
    fn parses_commands_and_rejects_invalid_arguments() -> Result<()> {
        assert!(matches!(parse_arguments(vec![])?, Command::Ui));

        match parse_arguments(vec!["send", "file.txt"])? {
            Command::Send {
                filename,
                endpoint_addr,
            } => {
                assert_eq!(filename, "file.txt");
                assert!(endpoint_addr.is_none());
            }
            _ => panic!("expected send command"),
        }

        assert!(matches!(
            parse_arguments(vec!["receive", "."] )?,
            Command::Receive { download_dir } if download_dir.is_absolute()
        ));
        assert!(matches!(parse_arguments(vec!["--help"])?, Command::Help));
        assert!(matches!(
            parse_arguments(vec!["--version"])?,
            Command::Version
        ));

        for args in [
            vec!["send"],
            vec!["send", "a", "b", "c"],
            vec!["receive", "a", "b"],
            vec!["unknown"],
            vec!["send", "file.txt", "invalid-ticket"],
        ] {
            assert!(parse_arguments(args).is_err());
        }

        Ok(())
    }

    #[tokio::test]
    async fn parses_endpoint_ticket() -> Result<()> {
        let endpoint = Endpoint::bind(presets::Minimal).await?;
        let ticket = EndpointTicket::new(endpoint.addr()).to_string();

        let command = parse_arguments(vec!["send", "file.txt", &ticket])?;
        match command {
            Command::Send {
                endpoint_addr: Some(addr),
                ..
            } => assert_eq!(addr.id, endpoint.id()),
            _ => panic!("expected send command with endpoint address"),
        }

        endpoint.close().await;
        Ok(())
    }
}
