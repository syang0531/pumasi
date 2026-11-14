//! A compute shader that never ends, to see the task's timeout stop it:
//!
//!   pumasi exec --gpu --timeout 10s forever.wasm
//!
//! The GPU work of a wasm task runs in a separate process (1.10), which
//! the agent ends at the task's timeout, so the task stops as `timeout`
//! and the GPU is free for the next one. Without that, a shader like this
//! one keeps the GPU busy until the driver resets it.

use webgpu_examples::{Buffer, Shader, adapter, dispatch, json_field, wait};

// Every value it stores is odd, so the value it waits for never comes;
// the compiler cannot tell, so the loop stays.
const WGSL: &str = r#"
@group(0) @binding(0) var<storage, read_write> v: array<u32>;

@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
    var x = v[id.x] | 1u;
    loop {
        x = x * 2654435761u;
        v[id.x] = x;
        if (v[(id.x + 1u) % 256u] == 4294967294u) { break; }
    }
}
"#;

fn main() {
    let Some(info) = adapter() else {
        eprintln!("no GPU: run with `pumasi exec --gpu`, or as a job with `gpus = 1`");
        std::process::exit(2);
    };
    println!(
        "adapter: {}",
        json_field(&info, "name").unwrap_or_else(|| "?".into())
    );
    let run = || -> webgpu_examples::Result<()> {
        let shader = Shader::new(WGSL, "main")?;
        let data = Buffer::from_u32(&vec![0; 256])?;
        dispatch(&shader, &[&data], [4, 1, 1])?;
        println!("dispatched a shader that never ends; waiting for it");
        wait()?;
        Ok(())
    };
    if let Err(e) = run() {
        eprintln!("GPU error: {e}");
        std::process::exit(3);
    }
    println!("the shader ended (it should not have)");
    std::process::exit(1);
}
