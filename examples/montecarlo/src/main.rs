//! Monte Carlo estimate of pi, split into independent shards.
//!
//!   montecarlo sample <seed> <samples>   count points inside the unit circle
//!   montecarlo reduce                    read sample lines on stdin, print pi
//!
//! No dependencies: a fixed PRNG keeps every shard deterministic, so the
//! same shard gives the same answer on any machine.

use std::io::{BufRead, Write};

/// SplitMix64: small, fast, good enough for a demo.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("sample") => {
            let seed: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
            let samples: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
            let mut rng = Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ 0xDEAD_BEEF);
            let mut inside = 0u64;
            for _ in 0..samples {
                let (x, y) = (rng.next_f64(), rng.next_f64());
                if x * x + y * y <= 1.0 {
                    inside += 1;
                }
            }
            println!("{seed} {inside} {samples}");
        }
        Some("reduce") => {
            let (mut inside, mut total, mut shards) = (0u64, 0u64, 0u64);
            for line in std::io::stdin().lock().lines() {
                let line = line.expect("read stdin");
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() == 3 {
                    if let (Ok(i), Ok(n)) = (parts[1].parse::<u64>(), parts[2].parse::<u64>()) {
                        inside += i;
                        total += n;
                        shards += 1;
                    }
                }
            }
            if total == 0 {
                eprintln!("no sample lines on stdin");
                std::process::exit(1);
            }
            let pi = 4.0 * inside as f64 / total as f64;
            let _ = writeln!(std::io::stdout(), "pi ~= {pi:.6} from {total} samples in {shards} shards");
        }
        _ => {
            eprintln!("usage: montecarlo sample <seed> <samples> | montecarlo reduce");
            std::process::exit(2);
        }
    }
}
