/// !!! IMPORTANT !!!
///
/// Needs unstable tokio features. Set the following env var during compilation:
///
/// ```bash
/// export RUSTFLAGS="--cfg tokio_unstable"
/// cargo build
/// ```
///
/// More info: <https://docs.rs/tokio/latest/tokio/#unstable-features>
use std::time::Duration;

use tokio::runtime::{Builder, Handle, Runtime};

const THREAD_COUNT: usize = 100;
const THREAD_KEEP_ALIVE: Duration = Duration::from_secs(10);
const BURST_SLEEP: Duration = Duration::from_millis(100);
const BUMP_INTERVAL: Duration = Duration::from_millis(100);

fn main() {
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .max_blocking_threads(THREAD_COUNT)
        .thread_keep_alive(THREAD_KEEP_ALIVE)
        .build()
        .unwrap();

    // Task to monitor blocking thread count
    monitor(runtime.handle().clone());

    // Burst blocking tasks to grow blocking thread count
    burst(&runtime);

    // Every BUMP_INTERVAL, spawn one blocking task that does nothing
    // Because of this we will never release allocated blocking threads
    runtime.block_on(async {
        let mut interval = tokio::time::interval(BUMP_INTERVAL);

        loop {
            interval.tick().await;
            runtime.spawn_blocking(|| {});
        }
    });
}

fn burst(runtime: &Runtime) {
    for _ in 0..THREAD_COUNT {
        runtime.spawn_blocking(|| {
            std::thread::sleep(BURST_SLEEP);
        });
    }
}

fn monitor(handle: Handle) {
    std::thread::spawn(move || loop {
        let metrics = handle.metrics();
        dbg!(metrics.num_blocking_threads());
        std::thread::sleep(Duration::from_secs(1));
    });
}
