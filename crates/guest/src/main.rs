//! `pumasi-guest`: PID 1 inside a pumasi microVM. See the library docs for
//! the disk protocol. It must be a static Linux binary (build it for the
//! `*-unknown-linux-musl` target); the agent packs it into the initramfs.

#[cfg(target_os = "linux")]
mod init;

#[cfg(target_os = "linux")]
fn main() {
    if std::process::id() != 1 {
        eprintln!("pumasi-guest is the init process of a pumasi microVM; the agent runs it");
        std::process::exit(2);
    }
    init::main()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("pumasi-guest runs only inside a Linux microVM");
    std::process::exit(2);
}
