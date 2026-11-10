//! Windows only:
//!
//! - Windows gives a program's main thread 1 MiB of stack, which the CLI's
//!   argument parser and command dispatch (large enums, unoptimized in debug
//!   builds) can overflow. Ask for 16 MiB, like other platforms' defaults.
//! - Version information (product name and version in the file's
//!   properties), which code signing checks the binary against.

fn main() {
    let target = std::env::var("TARGET").unwrap_or_default();
    if target.contains("windows") {
        if target.contains("msvc") {
            println!("cargo:rustc-link-arg-bins=/STACK:16777216");
        } else {
            println!("cargo:rustc-link-arg-bins=-Wl,--stack,16777216");
        }
        version_info(target.contains("msvc"));
    }
    println!("cargo:rerun-if-changed=build.rs");
}

/// Writes a VERSIONINFO resource and links it into the binary. Required on
/// MSVC (the release builds, where the Windows SDK's rc.exe is always
/// there); best effort with MinGW.
fn version_info(required: bool) {
    let version = env!("CARGO_PKG_VERSION");
    let mut nums = version
        .split(['-', '+'])
        .next()
        .unwrap_or("0")
        .split('.')
        .map(|n| n.parse::<u16>().unwrap_or(0));
    let (major, minor, patch) = (
        nums.next().unwrap_or(0),
        nums.next().unwrap_or(0),
        nums.next().unwrap_or(0),
    );
    // Constants spelled out so no SDK header is needed: VOS_NT_WINDOWS32,
    // VFT_APP, VFT2_UNKNOWN; 0x409 / 1200 is US English, Unicode.
    let rc = format!(
        r#"1 VERSIONINFO
FILEVERSION {major},{minor},{patch},0
PRODUCTVERSION {major},{minor},{patch},0
FILEFLAGSMASK 0x3F
FILEFLAGS 0x0
FILEOS 0x40004
FILETYPE 0x1
FILESUBTYPE 0x0
BEGIN
  BLOCK "StringFileInfo"
  BEGIN
    BLOCK "040904B0"
    BEGIN
      VALUE "FileDescription", "Pumasi"
      VALUE "FileVersion", "{version}"
      VALUE "InternalName", "pumasi"
      VALUE "LegalCopyright", "Apache-2.0"
      VALUE "OriginalFilename", "pumasi.exe"
      VALUE "ProductName", "Pumasi"
      VALUE "ProductVersion", "{version}"
    END
  END
  BLOCK "VarFileInfo"
  BEGIN
    VALUE "Translation", 0x409, 1200
  END
END
"#
    );
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    let path = out.join("pumasi.rc");
    std::fs::write(&path, rc).expect("write pumasi.rc");
    let res = embed_resource::compile(&path, embed_resource::NONE);
    let res = if required {
        res.manifest_required()
    } else {
        res.manifest_optional()
    };
    if let Err(e) = res {
        panic!("Windows version information: {e}");
    }
}
