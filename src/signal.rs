use tokio_util::sync::CancellationToken;

pub async fn wait_for_ctrl_c(cancel: CancellationToken) {
    let first = tokio::signal::ctrl_c().await;
    if first.is_err() {
        return;
    }

    eprintln!("\n  ⚠ Cancelling download...");
    cancel.cancel();

    let second = tokio::signal::ctrl_c().await;
    if second.is_ok() {
        eprintln!("\n  ✖ Force quit.");
        std::process::exit(1);
    }
}

pub fn spawn_signal_handler(cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
    tokio::spawn(wait_for_ctrl_c(cancel))
}
