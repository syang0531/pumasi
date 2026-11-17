//! Mac sensors (1.14): CPU and GPU temperatures and GPU power, which
//! sysinfo and the I/O registry's accelerator statistics leave out.
//!
//! | | Temperatures | Power |
//! | --- | --- | --- |
//! | Intel Macs | the SMC (`AppleSMC` keys `TC0D`, `TC0P`, `TG0D`, ...) | the SMC (`PCPG`, `PG0R`, ...) |
//! | Apple Silicon | the HID temperature sensors (through sysinfo, by name) | IOReport's "Energy Model" channels |
//!
//! Everything that turns raw readings into numbers is plain code, tested
//! against recorded output on every OS; only reading the SMC and IOReport
//! is macOS-only. `pumasi sensors --raw` prints the readings in the same
//! format the tests read, so a Mac's output can become a test.

use std::collections::BTreeMap;

/// What the Mac's sensors say, each where it can be read.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MacSensors {
    /// The CPU die (or, without one, its proximity sensor), in °C.
    pub cpu_temp: Option<f32>,
    /// The integrated GPU (Intel's, in the CPU package), °C and watts.
    pub igpu_temp: Option<f32>,
    pub igpu_power: Option<f32>,
    /// The discrete GPU (AMD or NVIDIA) or Apple's GPU, °C and watts.
    pub gpu_temp: Option<f32>,
    pub gpu_power: Option<f32>,
    /// The CPU package, in watts.
    pub cpu_power: Option<f32>,
}

fn max(a: Option<f32>, b: f32) -> Option<f32> {
    Some(a.map_or(b, |a| a.max(b)))
}

fn plausible_temp(t: f32) -> bool {
    t.is_finite() && t > 1.0 && t < 150.0
}

fn plausible_power(w: f32) -> bool {
    w.is_finite() && (0.0..2000.0).contains(&w)
}

// ---------------------------------------------------------------------------
// The SMC (Intel Macs)
// ---------------------------------------------------------------------------

pub mod smc {
    //! SMC keys are four characters; each value comes with a four-character
    //! type that says how to read its bytes.

    use super::*;

    /// The keys read, by what they measure. Models have different subsets;
    /// a missing key is skipped.
    pub const CPU_DIE: &[&str] = &[
        "TC0D", "TC0E", "TC0F", "TC0H", "TCXC", "TCXc", "TC1C", "TC2C", "TC3C", "TC4C",
    ];
    pub const CPU_PROXIMITY: &[&str] = &["TC0P", "TC0p"];
    /// The Intel GPU in the CPU package (PECI).
    pub const IGPU_TEMP: &[&str] = &["TCGC", "TCGc"];
    /// A discrete GPU: die, then proximity and heatsink.
    pub const GPU_TEMP: &[&str] = &["TG0D", "TG0d", "TGDD", "TG1D", "TG0P", "TG0p", "TG0H"];
    pub const CPU_POWER: &[&str] = &["PCPT", "PCPC", "PC0C", "PCTR"];
    pub const IGPU_POWER: &[&str] = &["PCPG"];
    pub const GPU_POWER: &[&str] = &["PG0R", "PGTR", "PG0C"];

    /// Every key worth asking for.
    pub fn keys() -> impl Iterator<Item = &'static str> {
        [
            CPU_DIE,
            CPU_PROXIMITY,
            IGPU_TEMP,
            GPU_TEMP,
            CPU_POWER,
            IGPU_POWER,
            GPU_POWER,
        ]
        .into_iter()
        .flatten()
        .copied()
    }

    /// A key as the SMC wants it: its four bytes, big-endian.
    pub fn fourcc(s: &str) -> Option<u32> {
        let b = s.as_bytes();
        (b.len() == 4).then(|| u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// The four characters of a type code.
    pub fn type_name(code: u32) -> String {
        code.to_be_bytes()
            .iter()
            .map(|&c| if c.is_ascii_graphic() { c as char } else { ' ' })
            .collect::<String>()
            .trim_end()
            .to_string()
    }

    /// The value of `bytes` read as `ty`: the signed and unsigned
    /// fixed-point types (`sp78`, `fpe2`, ...), `flt` (a little-endian
    /// float), and the plain integers. `None` for anything else.
    pub fn decode(ty: &str, bytes: &[u8]) -> Option<f32> {
        let be16 = || -> Option<u16> { Some(u16::from_be_bytes(bytes.get(..2)?.try_into().ok()?)) };
        let ty = ty.trim();
        match ty {
            "flt" => Some(f32::from_le_bytes(bytes.get(..4)?.try_into().ok()?)),
            "ui8" | "ui8 " => bytes.first().map(|&b| b as f32),
            "si8" => bytes.first().map(|&b| b as i8 as f32),
            "ui16" => be16().map(|v| v as f32),
            "si16" => be16().map(|v| v as i16 as f32),
            "ui32" => Some(u32::from_be_bytes(bytes.get(..4)?.try_into().ok()?) as f32),
            _ => {
                // `spXY` / `fpXY`: signed or unsigned 16-bit fixed point with
                // Y fraction bits (hex digit), as in sp78 or fpe2.
                let t = ty.as_bytes();
                if t.len() != 4 || !(t[0] == b's' || t[0] == b'f') || t[1] != b'p' {
                    return None;
                }
                let frac = (t[3] as char).to_digit(16)?;
                let int = (t[2] as char).to_digit(16)?;
                let signed = t[0] == b's';
                if int + frac + u32::from(signed) != 16 {
                    return None;
                }
                let raw = be16()?;
                let v = if signed {
                    raw as i16 as f32
                } else {
                    raw as f32
                };
                Some(v / (1u32 << frac) as f32)
            }
        }
    }

    /// One reading: key, type and the bytes the SMC returned.
    #[derive(Debug, Clone, PartialEq)]
    pub struct Reading {
        pub key: String,
        pub ty: String,
        pub bytes: Vec<u8>,
    }

    impl Reading {
        pub fn value(&self) -> Option<f32> {
            decode(&self.ty, &self.bytes)
        }

        /// `TC0P  [sp78]  (bytes 2f 80)`: the line `pumasi sensors --raw`
        /// prints and [`parse_dump`] reads.
        pub fn line(&self) -> String {
            let bytes: Vec<String> = self.bytes.iter().map(|b| format!("{b:02x}")).collect();
            format!("{}  [{}]  (bytes {})", self.key, self.ty, bytes.join(" "))
        }
    }

    /// Readings from a dump: lines like `  TC0P  [sp78]  47.5 (bytes 2f 80)`
    /// (smcFanControl's `smc -l`, and `pumasi sensors --raw`). Other lines
    /// are skipped.
    pub fn parse_dump(text: &str) -> Vec<Reading> {
        let mut out = Vec::new();
        for line in text.lines() {
            let line = line.trim();
            let (Some(open), Some(close)) = (line.find('['), line.find(']')) else {
                continue;
            };
            if close < open {
                continue;
            }
            let key = line[..open].trim();
            let ty = line[open + 1..close].trim();
            let Some(b) = line.find("(bytes") else {
                continue;
            };
            let bytes: Option<Vec<u8>> = line[b + 6..]
                .trim_end_matches(')')
                .split_whitespace()
                .map(|h| u8::from_str_radix(h, 16).ok())
                .collect();
            let (Some(bytes), 4) = (bytes, key.len()) else {
                continue;
            };
            out.push(Reading {
                key: key.to_string(),
                ty: ty.to_string(),
                bytes,
            });
        }
        out
    }

    /// What the readings say.
    pub fn summarize(readings: &[Reading]) -> MacSensors {
        let values: BTreeMap<&str, f32> = readings
            .iter()
            .filter_map(|r| Some((r.key.as_str(), r.value()?)))
            .collect();
        let temp = |keys: &[&str]| {
            keys.iter()
                .filter_map(|k| values.get(k).copied())
                .filter(|t| plausible_temp(*t))
                .fold(None, max)
        };
        let power = |keys: &[&str]| {
            keys.iter()
                .filter_map(|k| values.get(k).copied())
                .find(|w| plausible_power(*w))
        };
        MacSensors {
            cpu_temp: temp(CPU_DIE).or_else(|| temp(CPU_PROXIMITY)),
            igpu_temp: temp(IGPU_TEMP),
            igpu_power: power(IGPU_POWER),
            gpu_temp: temp(GPU_TEMP),
            gpu_power: power(GPU_POWER),
            cpu_power: power(CPU_POWER),
        }
    }
}

// ---------------------------------------------------------------------------
// Apple Silicon: IOReport energy and HID temperature sensors
// ---------------------------------------------------------------------------

pub mod ioreport {
    //! IOReport's "Energy Model" group counts energy per part of the chip;
    //! two samples apart give power. The HID temperature sensors have
    //! names that say what they sit on.

    use super::*;

    /// One channel of a delta between two samples.
    #[derive(Debug, Clone, PartialEq)]
    pub struct Channel {
        pub group: String,
        pub name: String,
        pub unit: String,
        pub value: i64,
    }

    impl Channel {
        /// `Energy Model | GPU Energy | mJ | 1234`: the line
        /// `pumasi sensors --raw` prints and [`parse_dump`] reads.
        pub fn line(&self) -> String {
            format!(
                "{} | {} | {} | {}",
                self.group, self.name, self.unit, self.value
            )
        }
    }

    pub fn parse_dump(text: &str) -> Vec<Channel> {
        text.lines()
            .filter_map(|l| {
                let p: Vec<&str> = l.split('|').map(str::trim).collect();
                let [group, name, unit, value] = p.as_slice() else {
                    return None;
                };
                Some(Channel {
                    group: group.to_string(),
                    name: name.to_string(),
                    unit: unit.to_string(),
                    value: value.parse().ok()?,
                })
            })
            .collect()
    }

    fn joules(unit: &str, v: i64) -> Option<f64> {
        let scale = match unit.trim() {
            "J" => 1.0,
            "mJ" => 1e-3,
            "uJ" | "µJ" => 1e-6,
            "nJ" => 1e-9,
            _ => return None,
        };
        Some(v as f64 * scale)
    }

    /// CPU and GPU power from the energy counted over `secs`.
    pub fn power(channels: &[Channel], secs: f64) -> (Option<f32>, Option<f32>) {
        if secs <= 0.0 {
            return (None, None);
        }
        let (mut cpu, mut gpu) = (None::<f64>, None::<f64>);
        for c in channels.iter().filter(|c| c.group == "Energy Model") {
            let Some(j) = joules(&c.unit, c.value) else {
                continue;
            };
            // "CPU Energy" sums the clusters on every chip so far ("EACC_CPU",
            // "PACC0_CPU" ... are its parts); "GPU Energy" is the GPU.
            if c.name.ends_with("CPU Energy") {
                *cpu.get_or_insert(0.0) += j;
            } else if c.name == "GPU Energy" {
                *gpu.get_or_insert(0.0) += j;
            }
        }
        let w = |j: Option<f64>| j.map(|j| (j / secs) as f32).filter(|w| plausible_power(*w));
        (w(cpu), w(gpu))
    }

    /// What a HID temperature sensor sits on, by its name.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Part {
        Cpu,
        Gpu,
        Other,
    }

    pub fn part_of(name: &str) -> Part {
        let n = name.to_ascii_lowercase();
        if n.starts_with("gpu") || n.contains("gpu mtr") {
            Part::Gpu
        } else if n.contains("tdie")
            || n.starts_with("pacc")
            || n.starts_with("eacc")
            || n.starts_with("cpu")
            || n.contains("peci cpu")
            || n == "cpu proximity"
        {
            Part::Cpu
        } else {
            Part::Other
        }
    }

    /// CPU and GPU temperatures from named sensors (the hottest of each).
    pub fn temps<'a>(
        sensors: impl IntoIterator<Item = (&'a str, f32)>,
    ) -> (Option<f32>, Option<f32>) {
        let (mut cpu, mut gpu) = (None, None);
        for (name, t) in sensors {
            if !plausible_temp(t) {
                continue;
            }
            match part_of(name) {
                Part::Cpu => cpu = max(cpu, t),
                Part::Gpu => gpu = max(gpu, t),
                Part::Other => {}
            }
        }
        (cpu, gpu)
    }
}

/// Reads the Mac's sensors; elsewhere reads nothing.
pub struct Reader {
    #[cfg(target_os = "macos")]
    inner: mac::Reader,
}

impl Default for Reader {
    fn default() -> Self {
        Self::new()
    }
}

impl Reader {
    pub fn new() -> Self {
        Self {
            #[cfg(target_os = "macos")]
            inner: mac::Reader::new(),
        }
    }

    /// What the sensors say now. `hid` is sysinfo's sensors by name (the
    /// HID sensors on Apple Silicon).
    pub fn read<'a>(&mut self, hid: impl IntoIterator<Item = (&'a str, f32)>) -> MacSensors {
        #[cfg(target_os = "macos")]
        {
            self.inner.read(hid)
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = hid;
            MacSensors::default()
        }
    }

    /// The raw readings, in the formats [`smc::parse_dump`] and
    /// [`ioreport::parse_dump`] read.
    pub fn raw(&mut self) -> Vec<String> {
        #[cfg(target_os = "macos")]
        {
            self.inner.raw()
        }
        #[cfg(not(target_os = "macos"))]
        {
            Vec::new()
        }
    }
}

#[cfg(target_os = "macos")]
mod mac {
    use std::time::Instant;

    use super::{MacSensors, ioreport, smc};

    pub struct Reader {
        smc: Option<smc_io::Smc>,
        ioreport: Option<ior::Sampler>,
        last: Option<(Instant, ior::Sample)>,
    }

    impl Reader {
        pub fn new() -> Self {
            // Intel Macs: the SMC has it all. Apple Silicon: IOReport for
            // power, the HID sensors (sysinfo) for temperatures.
            let smc = if cfg!(target_arch = "x86_64") {
                smc_io::Smc::open()
            } else {
                None
            };
            let ioreport = if cfg!(target_arch = "aarch64") {
                ior::Sampler::open()
            } else {
                None
            };
            Self {
                smc,
                ioreport,
                last: None,
            }
        }

        fn smc_readings(&mut self) -> Vec<smc::Reading> {
            let Some(s) = self.smc.as_mut() else {
                return Vec::new();
            };
            smc::keys().filter_map(|k| s.read(k)).collect()
        }

        /// The energy since the last call, as channels.
        fn energy(&mut self) -> Option<(Vec<ioreport::Channel>, f64)> {
            let s = self.ioreport.as_ref()?;
            let now = s.sample()?;
            let at = Instant::now();
            let prev = self.last.replace((at, now));
            let (then, prev) = prev?;
            let cur = &self.last.as_ref()?.1;
            Some((s.delta(&prev, cur), at.duration_since(then).as_secs_f64()))
        }

        pub fn read<'a>(&mut self, hid: impl IntoIterator<Item = (&'a str, f32)>) -> MacSensors {
            let mut m = smc::summarize(&self.smc_readings());
            if self.ioreport.is_some() {
                let (cpu_t, gpu_t) = ioreport::temps(hid);
                m.cpu_temp = m.cpu_temp.or(cpu_t);
                m.gpu_temp = m.gpu_temp.or(gpu_t);
                if let Some((channels, secs)) = self.energy() {
                    let (cpu_w, gpu_w) = ioreport::power(&channels, secs);
                    m.cpu_power = m.cpu_power.or(cpu_w);
                    m.gpu_power = m.gpu_power.or(gpu_w);
                }
            }
            m
        }

        pub fn raw(&mut self) -> Vec<String> {
            let mut out: Vec<String> = self.smc_readings().iter().map(|r| r.line()).collect();
            if self.ioreport.is_some() {
                let _ = self.energy();
                std::thread::sleep(std::time::Duration::from_secs(1));
                if let Some((channels, _)) = self.energy() {
                    out.extend(channels.iter().map(|c| c.line()));
                }
            }
            out
        }
    }

    /// CoreFoundation, as both readers need it.
    pub(super) mod cf {
        use std::ffi::{CString, c_char, c_void};

        pub type CfRef = *const c_void;
        pub const UTF8: u32 = 0x0800_0100;

        #[link(name = "CoreFoundation", kind = "framework")]
        unsafe extern "C" {
            pub fn CFStringCreateWithCString(
                alloc: CfRef,
                s: *const c_char,
                encoding: u32,
            ) -> CfRef;
            pub fn CFStringGetCString(
                s: CfRef,
                buf: *mut c_char,
                len: isize,
                encoding: u32,
            ) -> bool;
            pub fn CFRelease(cf: CfRef);
            pub fn CFDictionaryGetValue(dict: CfRef, key: CfRef) -> CfRef;
            pub fn CFDictionaryGetCount(dict: CfRef) -> isize;
            pub fn CFDictionaryCreateMutableCopy(alloc: CfRef, cap: isize, dict: CfRef) -> CfRef;
            pub fn CFArrayGetCount(a: CfRef) -> isize;
            pub fn CFArrayGetValueAtIndex(a: CfRef, i: isize) -> CfRef;
        }

        /// A CoreFoundation object we own.
        pub struct Owned(pub CfRef);

        impl Drop for Owned {
            fn drop(&mut self) {
                if !self.0.is_null() {
                    // SAFETY: created or copied by us, released once.
                    unsafe { CFRelease(self.0) };
                }
            }
        }

        // SAFETY: CoreFoundation objects may be used and released from any
        // thread; the sampler keeps them behind `&mut self`.
        unsafe impl Send for Owned {}

        pub fn string(s: &str) -> Option<Owned> {
            let c = CString::new(s).ok()?;
            // SAFETY: a valid C string.
            let cf = unsafe { CFStringCreateWithCString(std::ptr::null(), c.as_ptr(), UTF8) };
            (!cf.is_null()).then_some(Owned(cf))
        }

        /// A borrowed CFString's text.
        pub fn text(s: CfRef) -> String {
            if s.is_null() {
                return String::new();
            }
            let mut buf = [0 as c_char; 256];
            // SAFETY: `s` is a CFString; the buffer's size is passed.
            let ok = unsafe { CFStringGetCString(s, buf.as_mut_ptr(), buf.len() as isize, UTF8) };
            if !ok {
                return String::new();
            }
            // SAFETY: NUL-terminated by CFStringGetCString.
            unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) }
                .to_string_lossy()
                .into_owned()
        }
    }

    /// The SMC through its I/O Kit user client.
    mod smc_io {
        use std::{collections::HashMap, ffi::c_void};

        use super::smc;

        #[repr(C)]
        #[derive(Default, Clone, Copy)]
        struct Vers {
            major: u8,
            minor: u8,
            build: u8,
            reserved: u8,
            release: u16,
        }

        #[repr(C)]
        #[derive(Default, Clone, Copy)]
        struct PLimit {
            version: u16,
            length: u16,
            cpu: u32,
            gpu: u32,
            mem: u32,
        }

        #[repr(C)]
        #[derive(Default, Clone, Copy)]
        struct KeyInfo {
            data_size: u32,
            data_type: u32,
            attributes: u8,
        }

        /// `SMCKeyData_t`, 80 bytes.
        #[repr(C)]
        #[derive(Default, Clone, Copy)]
        struct KeyData {
            key: u32,
            vers: Vers,
            plimit: PLimit,
            info: KeyInfo,
            result: u8,
            status: u8,
            data8: u8,
            data32: u32,
            bytes: [u8; 32],
        }

        const _: () = assert!(std::mem::size_of::<KeyData>() == 80);

        const KERNEL_INDEX_SMC: u32 = 2;
        const CMD_READ_BYTES: u8 = 5;
        const CMD_READ_KEYINFO: u8 = 9;

        #[link(name = "IOKit", kind = "framework")]
        unsafe extern "C" {
            fn IOServiceMatching(name: *const std::ffi::c_char) -> *mut c_void;
            fn IOServiceGetMatchingService(main_port: u32, matching: *mut c_void) -> u32;
            fn IOServiceOpen(service: u32, task: u32, kind: u32, connect: *mut u32) -> i32;
            fn IOServiceClose(connect: u32) -> i32;
            fn IOObjectRelease(obj: u32) -> i32;
            fn IOConnectCallStructMethod(
                connect: u32,
                selector: u32,
                input: *const c_void,
                input_size: usize,
                output: *mut c_void,
                output_size: *mut usize,
            ) -> i32;
        }

        unsafe extern "C" {
            /// This task's port (what `mach_task_self()` returns).
            static mach_task_self_: u32;
        }

        pub struct Smc {
            conn: u32,
            /// Each key's size and type, or none when the SMC lacks it.
            info: HashMap<u32, Option<KeyInfo>>,
        }

        impl Drop for Smc {
            fn drop(&mut self) {
                // SAFETY: opened in `open`, closed once.
                unsafe { IOServiceClose(self.conn) };
            }
        }

        // SAFETY: the connection is a plain port name, usable from any thread.
        unsafe impl Send for Smc {}

        impl Smc {
            pub fn open() -> Option<Self> {
                // SAFETY: plain I/O Kit calls; the service is released and
                // the connection closed on drop.
                unsafe {
                    let matching = IOServiceMatching(c"AppleSMC".as_ptr());
                    if matching.is_null() {
                        return None;
                    }
                    // Consumes `matching`.
                    let service = IOServiceGetMatchingService(0, matching);
                    if service == 0 {
                        return None;
                    }
                    let mut conn = 0u32;
                    let r = IOServiceOpen(service, mach_task_self_, 0, &mut conn);
                    IOObjectRelease(service);
                    (r == 0 && conn != 0).then(|| Self {
                        conn,
                        info: HashMap::new(),
                    })
                }
            }

            fn call(&self, input: &KeyData) -> Option<KeyData> {
                let mut out = KeyData::default();
                let mut size = std::mem::size_of::<KeyData>();
                // SAFETY: both structures are `SMCKeyData_t`-sized.
                let r = unsafe {
                    IOConnectCallStructMethod(
                        self.conn,
                        KERNEL_INDEX_SMC,
                        input as *const KeyData as *const c_void,
                        std::mem::size_of::<KeyData>(),
                        &mut out as *mut KeyData as *mut c_void,
                        &mut size,
                    )
                };
                (r == 0 && out.result == 0).then_some(out)
            }

            pub fn read(&mut self, key: &str) -> Option<smc::Reading> {
                let code = smc::fourcc(key)?;
                let info = match self.info.get(&code) {
                    Some(i) => (*i)?,
                    None => {
                        let i = self
                            .call(&KeyData {
                                key: code,
                                data8: CMD_READ_KEYINFO,
                                ..Default::default()
                            })
                            .map(|o| o.info)
                            .filter(|i| i.data_size > 0 && i.data_size <= 32);
                        self.info.insert(code, i);
                        i?
                    }
                };
                let out = self.call(&KeyData {
                    key: code,
                    info,
                    data8: CMD_READ_BYTES,
                    ..Default::default()
                })?;
                Some(smc::Reading {
                    key: key.to_string(),
                    ty: smc::type_name(info.data_type),
                    bytes: out.bytes[..info.data_size as usize].to_vec(),
                })
            }
        }
    }

    /// IOReport, loaded at run time (a private library, not on every
    /// macOS).
    mod ior {
        use std::ffi::c_void;

        use super::{
            cf::{self, CfRef, Owned},
            ioreport::Channel,
        };

        type CopyChannelsInGroup = unsafe extern "C" fn(CfRef, CfRef, u64, u64, u64) -> CfRef;
        type CreateSubscription =
            unsafe extern "C" fn(*const c_void, CfRef, *mut CfRef, u64, CfRef) -> *const c_void;
        type CreateSamples = unsafe extern "C" fn(*const c_void, CfRef, CfRef) -> CfRef;
        type CreateSamplesDelta = unsafe extern "C" fn(CfRef, CfRef, CfRef) -> CfRef;
        type GetString = unsafe extern "C" fn(CfRef) -> CfRef;
        type GetInteger = unsafe extern "C" fn(CfRef, i32) -> i64;

        struct Lib {
            create_samples: CreateSamples,
            create_delta: CreateSamplesDelta,
            group: GetString,
            name: GetString,
            unit: GetString,
            integer: GetInteger,
        }

        /// One sample of the subscribed channels.
        pub struct Sample(Owned);

        pub struct Sampler {
            lib: Lib,
            subscription: *const c_void,
            channels: Owned,
        }

        // SAFETY: used behind `&mut` from one thread at a time.
        unsafe impl Send for Sampler {}

        fn sym<T: Copy>(handle: *mut c_void, name: &std::ffi::CStr) -> Option<T> {
            // SAFETY: a symbol of the library we opened; `T` is the
            // function type its users declare.
            let p = unsafe { libc::dlsym(handle, name.as_ptr()) };
            (!p.is_null()).then(|| unsafe { std::mem::transmute_copy::<*mut c_void, T>(&p) })
        }

        impl Sampler {
            pub fn open() -> Option<Self> {
                // SAFETY: dlopen of a system library; never closed.
                let h = unsafe {
                    libc::dlopen(
                        c"/usr/lib/libIOReport.dylib".as_ptr(),
                        libc::RTLD_NOW | libc::RTLD_LOCAL,
                    )
                };
                if h.is_null() {
                    return None;
                }
                let copy: CopyChannelsInGroup = sym(h, c"IOReportCopyChannelsInGroup")?;
                let subscribe: CreateSubscription = sym(h, c"IOReportCreateSubscription")?;
                let lib = Lib {
                    create_samples: sym(h, c"IOReportCreateSamples")?,
                    create_delta: sym(h, c"IOReportCreateSamplesDelta")?,
                    group: sym(h, c"IOReportChannelGetGroup")?,
                    name: sym(h, c"IOReportChannelGetChannelName")?,
                    unit: sym(h, c"IOReportChannelGetUnitLabel")?,
                    integer: sym(h, c"IOReportSimpleGetIntegerValue")?,
                };
                let group = cf::string("Energy Model")?;
                // SAFETY: IOReport's documented-by-use calls; what they
                // create is owned and released here.
                unsafe {
                    let found = Owned(copy(group.0, std::ptr::null(), 0, 0, 0));
                    if found.0.is_null() {
                        return None;
                    }
                    let n = cf::CFDictionaryGetCount(found.0);
                    let desired = Owned(cf::CFDictionaryCreateMutableCopy(
                        std::ptr::null(),
                        n,
                        found.0,
                    ));
                    if desired.0.is_null() {
                        return None;
                    }
                    let mut subbed: CfRef = std::ptr::null();
                    let subscription = subscribe(
                        std::ptr::null(),
                        desired.0,
                        &mut subbed,
                        0,
                        std::ptr::null(),
                    );
                    if subscription.is_null() || subbed.is_null() {
                        return None;
                    }
                    Some(Self {
                        lib,
                        subscription,
                        channels: Owned(subbed),
                    })
                }
            }

            pub fn sample(&self) -> Option<Sample> {
                // SAFETY: the subscription and its channels from `open`.
                let s = unsafe {
                    (self.lib.create_samples)(self.subscription, self.channels.0, std::ptr::null())
                };
                (!s.is_null()).then(|| Sample(Owned(s)))
            }

            /// The channels' change from `a` to `b`.
            pub fn delta(&self, a: &Sample, b: &Sample) -> Vec<Channel> {
                let mut out = Vec::new();
                // SAFETY: both samples are IOReport sample dictionaries; the
                // delta is owned and released here, what it holds borrowed.
                unsafe {
                    let d = Owned((self.lib.create_delta)(a.0.0, b.0.0, std::ptr::null()));
                    if d.0.is_null() {
                        return out;
                    }
                    let Some(key) = cf::string("IOReportChannels") else {
                        return out;
                    };
                    let items = cf::CFDictionaryGetValue(d.0, key.0);
                    if items.is_null() {
                        return out;
                    }
                    for i in 0..cf::CFArrayGetCount(items) {
                        let ch = cf::CFArrayGetValueAtIndex(items, i);
                        if ch.is_null() {
                            continue;
                        }
                        out.push(Channel {
                            group: cf::text((self.lib.group)(ch)),
                            name: cf::text((self.lib.name)(ch)),
                            unit: cf::text((self.lib.unit)(ch)),
                            value: (self.lib.integer)(ch, 0),
                        });
                    }
                }
                out
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 2014 iMac (Intel Core i5, AMD Radeon R9 M290X): readings in the
    /// format of `smc -l` (smcFanControl's tool).
    const IMAC_2014_SMC: &str = "
  TA0P  [sp78]  27.6 (bytes 1b 9a)
  TC0D  [sp78]  52.5 (bytes 34 80)
  TC0E  [sp78]  53.2 (bytes 35 32)
  TC0F  [sp78]  54.8 (bytes 36 cc)
  TC0P  [sp78]  44.9 (bytes 2c e6)
  TCGC  [sp78]  50.0 (bytes 32 00)
  TG0D  [sp78]  61.0 (bytes 3d 00)
  TG0P  [sp78]  55.4 (bytes 37 66)
  PC0C  [sp96]  11.5 (bytes 02 e0)
  PCPC  [sp96]  9.8 (bytes 02 73)
  PCPG  [sp96]  0.5 (bytes 00 20)
  PCPT  [sp96]  12.1 (bytes 03 06)
  PG0R  [sp87]  21.2 (bytes 0a 9a)
  F0Ac  [fpe2]  1199 (bytes 12 bc)
  #KEY  [ui32]  (bytes 00 00 02 1c)
  BAD!  [sp78]  (bytes zz)
";

    /// A MacBook Pro with only an Intel GPU, whose newer SMC reports
    /// floats; one sensor missing reads 0 (or worse) and is ignored.
    const MBP_2017_SMC: &str = "
TC0P  [flt ]  (bytes 00 00 48 42)
TCXC  [flt ]  (bytes 00 00 5c 42)
TC1C  [flt ]  (bytes 00 00 60 42)
TCGC  [flt ]  (bytes 00 00 54 42)
TG0P  [flt ]  (bytes 00 00 00 00)
PCPT  [flt ]  (bytes 00 00 a0 41)
PCPG  [flt ]  (bytes 00 00 40 40)
";

    /// IOReport's "Energy Model" over one second on an M1 (as
    /// `pumasi sensors --raw` prints it).
    const M1_IOREPORT: &str = "
Energy Model | EACC_CPU | mJ | 120
Energy Model | PACC0_CPU | mJ | 2380
Energy Model | CPU Energy | mJ | 2500
Energy Model | GPU Energy | mJ | 1750
Energy Model | GPU0 | mJ | 1750
Energy Model | ANE | mJ | 0
Energy Model | DRAM | mJ | 310
CPU Stats | CPU Core Performance States | ms | 999
";

    #[test]
    fn smc_types_decode() {
        assert_eq!(smc::decode("sp78", &[0x2f, 0x80]), Some(47.5));
        assert_eq!(smc::decode("sp78", &[0xff, 0x00]), Some(-1.0));
        assert_eq!(smc::decode("fpe2", &[0x12, 0xbc]), Some(1199.0));
        assert_eq!(smc::decode("sp96", &[0x02, 0x73]), Some(9.796875));
        assert_eq!(smc::decode("flt", &[0x00, 0x00, 0x48, 0x42]), Some(50.0));
        assert_eq!(smc::decode("flt ", &[0x00, 0x00, 0x48, 0x42]), Some(50.0));
        assert_eq!(smc::decode("ui16", &[0x01, 0x00]), Some(256.0));
        assert_eq!(smc::decode("si8", &[0xfe]), Some(-2.0));
        // Not fixed point: the parts do not add up to 16 bits.
        assert_eq!(smc::decode("sp77", &[0x10, 0x00]), None);
        assert_eq!(smc::decode("ch8*", b"abcd"), None);
        assert_eq!(smc::decode("sp78", &[0x10]), None);
        assert_eq!(smc::fourcc("TC0P"), Some(0x5443_3050));
        assert_eq!(smc::type_name(u32::from_be_bytes(*b"flt ")), "flt");
        assert_eq!(smc::type_name(u32::from_be_bytes(*b"sp78")), "sp78");
    }

    #[test]
    fn imac_2014_from_its_smc() {
        let r = smc::parse_dump(IMAC_2014_SMC);
        assert_eq!(r.len(), 15, "the bad line is skipped: {r:?}");
        assert_eq!(r[0].line(), "TA0P  [sp78]  (bytes 1b 9a)");
        assert_eq!(smc::parse_dump(&r[0].line()), vec![r[0].clone()]);
        let m = smc::summarize(&r);
        // The hottest die sensor, not the proximity one.
        assert_eq!(m.cpu_temp, Some(54.796875));
        assert_eq!(m.igpu_temp, Some(50.0));
        assert_eq!(m.gpu_temp, Some(61.0));
        assert_eq!(m.cpu_power, Some(12.09375));
        assert_eq!(m.igpu_power, Some(0.5));
        assert!((m.gpu_power.unwrap() - 21.2).abs() < 0.01, "{m:?}");
    }

    #[test]
    fn floats_and_missing_sensors() {
        let m = smc::summarize(&smc::parse_dump(MBP_2017_SMC));
        assert_eq!(m.cpu_temp, Some(56.0));
        assert_eq!(m.igpu_temp, Some(53.0));
        // TG0P reads 0: no discrete GPU.
        assert_eq!(m.gpu_temp, None);
        assert_eq!(m.cpu_power, Some(20.0));
        assert_eq!(m.igpu_power, Some(3.0));
        assert_eq!(m.gpu_power, None);
        // Only a proximity sensor.
        let m = smc::summarize(&smc::parse_dump("TC0P  [sp78]  (bytes 2c 80)"));
        assert_eq!(m.cpu_temp, Some(44.5));
        assert_eq!(smc::summarize(&[]), MacSensors::default());
    }

    #[test]
    fn apple_silicon_power_from_ioreport() {
        let ch = ioreport::parse_dump(M1_IOREPORT);
        assert_eq!(ch.len(), 8);
        assert_eq!(ioreport::parse_dump(&ch[3].line()), vec![ch[3].clone()]);
        let (cpu, gpu) = ioreport::power(&ch, 1.0);
        // "CPU Energy" only: its parts are not counted twice.
        assert_eq!(cpu, Some(2.5));
        assert_eq!(gpu, Some(1.75));
        let (cpu, gpu) = ioreport::power(&ch, 0.5);
        assert_eq!((cpu, gpu), (Some(5.0), Some(3.5)));
        assert_eq!(ioreport::power(&ch, 0.0), (None, None));
        let nj = ioreport::parse_dump("Energy Model | GPU Energy | nJ | 2000000000");
        assert_eq!(ioreport::power(&nj, 2.0), (None, Some(1.0)));
    }

    #[test]
    fn apple_silicon_temperatures_by_name() {
        // Names the HID sensors have on M1 and M2 Macs.
        let sensors = [
            ("PMU tdie1", 48.0),
            ("PMU tdie8", 51.5),
            ("pACC MTR Temp Sensor2", 53.0),
            ("eACC MTR Temp Sensor0", 41.0),
            ("GPU MTR Temp Sensor1", 45.5),
            ("GPU MTR Temp Sensor4", 47.0),
            ("NAND CH0 temp", 39.0),
            ("gas gauge battery", 31.0),
            ("PMU tcal", 0.0),
        ];
        assert_eq!(ioreport::temps(sensors), (Some(53.0), Some(47.0)));
        assert_eq!(ioreport::part_of("NAND CH0 temp"), ioreport::Part::Other);
        assert_eq!(ioreport::part_of("CPU Proximity"), ioreport::Part::Cpu);
        assert_eq!(ioreport::part_of("GPU"), ioreport::Part::Gpu);
    }

    #[test]
    fn reads_nothing_off_a_mac() {
        let mut r = Reader::new();
        let m = r.read([("PMU tdie1", 50.0)]);
        if !cfg!(target_os = "macos") {
            assert_eq!(m, MacSensors::default());
            assert!(r.raw().is_empty());
        }
    }
}
