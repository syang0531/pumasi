//! Windows temperatures (1.6, more sources in 1.17).
//!
//! Windows has no API for CPU temperatures: reading them takes a kernel
//! driver, and Pumasi ships none. The agent reads what is there:
//!
//! | Source | Needs | What |
//! | --- | --- | --- |
//! | ACPI thermal zones (`root\WMI`) | administrator (the service) | the zones the firmware fills, often none on desktops |
//! | Thermal Zone Information counters (`root\cimv2`, 1.17) | nothing | the same zones, for agents without administrator rights |
//! | LibreHardwareMonitor (`root\LibreHardwareMonitor`) | it running | CPU, GPU and drive sensors |
//! | Open Hardware Monitor (`root\OpenHardwareMonitor`, 1.17) | it running | the same, for machines that still have it |
//! | HWiNFO (`Global\HWiNFO_SENS_SM2`, 1.17) | it running with "Shared Memory Support" | every sensor it reads |
//!
//! The last three are the opt-in ways (README "Windows temperatures"):
//! the owner installs a monitor, which brings its own driver. Parsing is
//! plain code tested on every OS; only reading is Windows-only.

/// One temperature a source read.
#[derive(Debug, Clone, PartialEq)]
pub struct Reading {
    /// `acpi`, `zone`, `lhm`, `ohm` or `hwinfo`.
    pub source: &'static str,
    pub name: String,
    pub celsius: f32,
}

impl Reading {
    /// The line `pumasi sensors --raw` prints.
    pub fn line(&self) -> String {
        format!("{:<7} {:>6.1} °C  {}", self.source, self.celsius, self.name)
    }
}

/// Sensors that read as temperatures but are not: how far a core is from
/// its limit ("Distance to TjMax", which LibreHardwareMonitor and HWiNFO
/// both list), and the limit itself.
pub fn is_temperature(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    !(n.contains("distance") || n.contains("tjmax") || n.contains("to tj") || n.contains("limit"))
}

/// The hottest plausible reading.
pub fn hottest<'a>(readings: impl IntoIterator<Item = &'a Reading>) -> Option<f32> {
    readings
        .into_iter()
        .map(|r| r.celsius)
        .filter(|t| t.is_finite() && *t > 1.0 && *t < 150.0)
        .fold(None, |a: Option<f32>, t| Some(a.map_or(t, |a| a.max(t))))
}

/// HWiNFO's shared memory (version 2 of its layout, `HWiNFO_SENS_SM2`).
pub mod hwinfo {
    use anyhow::{Result, bail, ensure};

    use super::Reading;

    /// The mapping's name; `Global\` so the agent service (session 0)
    /// sees what HWiNFO in a desktop session shares.
    pub const MAPPING: &str = "Global\\HWiNFO_SENS_SM2";
    /// `HWiS` while HWiNFO runs, `DEAD` once it has stopped sharing.
    pub const SIGNATURE: u32 = u32::from_le_bytes(*b"HWiS");
    pub const DEAD: u32 = u32::from_le_bytes(*b"DEAD");
    /// `SENSOR_TYPE_TEMP`.
    pub const TEMPERATURE: u32 = 1;
    const HEADER: usize = 44;
    const SENSOR: usize = 264;
    const READING: usize = 292;
    /// More than any machine has; a bound on a damaged header.
    const MAX_ELEMENTS: u32 = 16_384;

    fn u32_at(b: &[u8], at: usize) -> Option<u32> {
        Some(u32::from_le_bytes(b.get(at..at + 4)?.try_into().ok()?))
    }

    fn f64_at(b: &[u8], at: usize) -> Option<f64> {
        Some(f64::from_le_bytes(b.get(at..at + 8)?.try_into().ok()?))
    }

    /// A NUL-ended ANSI string (Windows-1252, read as Latin-1, which
    /// covers the degree sign).
    fn text(b: &[u8]) -> String {
        let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
        b[..end].iter().map(|&c| c as char).collect()
    }

    /// The temperatures in a copy of the shared memory: each named
    /// `sensor: label` (the names the owner gave them, if any).
    pub fn parse(b: &[u8]) -> Result<Vec<Reading>> {
        ensure!(b.len() >= HEADER, "HWiNFO's shared memory is too short");
        match u32_at(b, 0) {
            Some(SIGNATURE) => {}
            Some(DEAD) => bail!("HWiNFO stopped sharing its sensors"),
            _ => bail!("not HWiNFO's shared memory"),
        }
        let field = |i: usize| u32_at(b, 20 + 4 * i).unwrap_or(0);
        let (s_off, s_size, s_n) = (field(0) as usize, field(1) as usize, field(2));
        let (r_off, r_size, r_n) = (field(3) as usize, field(4) as usize, field(5));
        ensure!(
            s_n <= MAX_ELEMENTS && r_n <= MAX_ELEMENTS,
            "HWiNFO's header is damaged"
        );
        ensure!(
            (s_n == 0 || s_size >= SENSOR) && (r_n == 0 || r_size >= READING),
            "HWiNFO's elements are smaller than its layout"
        );
        ensure!(
            s_off.saturating_add(s_size.saturating_mul(s_n as usize)) <= b.len()
                && r_off.saturating_add(r_size.saturating_mul(r_n as usize)) <= b.len(),
            "HWiNFO's elements run past its shared memory"
        );
        let sensor = |i: usize| -> String {
            let e = &b[s_off + i * s_size..][..SENSOR];
            let user = text(&e[136..264]);
            if user.is_empty() {
                text(&e[8..136])
            } else {
                user
            }
        };
        let mut out = Vec::new();
        for i in 0..r_n as usize {
            let at = r_off + i * r_size;
            let e = &b[at..at + READING];
            if u32_at(e, 0) != Some(TEMPERATURE) {
                continue;
            }
            let s = u32_at(e, 4).unwrap_or(u32::MAX);
            let label = match text(&e[140..268]) {
                l if l.is_empty() => text(&e[12..140]),
                l => l,
            };
            let unit = text(&e[268..284]);
            let Some(mut v) = f64_at(e, 284) else {
                continue;
            };
            if unit.trim_end().ends_with('F') {
                v = (v - 32.0) * 5.0 / 9.0;
            }
            let name = if (s as usize) < s_n as usize {
                format!("{}: {label}", sensor(s as usize))
            } else {
                label
            };
            out.push(Reading {
                source: "hwinfo",
                name,
                celsius: v as f32,
            });
        }
        Ok(out)
    }

    /// What HWiNFO shares now, if it runs with shared memory on.
    #[cfg(windows)]
    pub fn read() -> Option<Vec<Reading>> {
        use windows_sys::Win32::{
            Foundation::CloseHandle,
            System::Memory::{
                FILE_MAP_READ, MEMORY_BASIC_INFORMATION, MEMORY_MAPPED_VIEW_ADDRESS, MapViewOfFile,
                OpenFileMappingW, UnmapViewOfFile, VirtualQuery,
            },
        };
        let name: Vec<u16> = MAPPING.encode_utf16().chain([0]).collect();
        // SAFETY: a NUL-terminated name; the handle is closed below.
        let handle = unsafe { OpenFileMappingW(FILE_MAP_READ, 0, name.as_ptr()) };
        if handle.is_null() {
            return None;
        }
        // SAFETY: a read-only view of the whole mapping, unmapped below;
        // its size comes from VirtualQuery, and the bytes are copied out
        // before the view goes.
        let bytes = unsafe {
            let view = MapViewOfFile(handle, FILE_MAP_READ, 0, 0, 0);
            CloseHandle(handle);
            if view.Value.is_null() {
                return None;
            }
            let mut info: MEMORY_BASIC_INFORMATION = std::mem::zeroed();
            let got = VirtualQuery(
                view.Value,
                &mut info,
                std::mem::size_of::<MEMORY_BASIC_INFORMATION>(),
            );
            let len = if got == 0 { 0 } else { info.RegionSize };
            let bytes = std::slice::from_raw_parts(view.Value as *const u8, len).to_vec();
            UnmapViewOfFile(MEMORY_MAPPED_VIEW_ADDRESS { Value: view.Value });
            bytes
        };
        match parse(&bytes) {
            Ok(r) => Some(r),
            Err(e) => {
                tracing::debug!("HWiNFO: {e:#}");
                None
            }
        }
    }

    #[cfg(test)]
    pub(crate) mod tests {
        use super::*;

        /// Shared memory as HWiNFO lays it out: `sensors` by name, then
        /// `(type, sensor, label, unit, value)` readings.
        pub fn memory(sensors: &[&str], readings: &[(u32, u32, &str, &[u8], f64)]) -> Vec<u8> {
            // HWiNFO 7 adds UTF-8 names after the ANSI ones; the element
            // sizes in the header say so, and the parser steps by them.
            let (s_size, r_size) = (SENSOR + 128, READING + 32 + 144);
            let s_off = HEADER + 4;
            let r_off = s_off + s_size * sensors.len();
            let mut b = vec![0u8; r_off + r_size * readings.len()];
            b[0..4].copy_from_slice(&SIGNATURE.to_le_bytes());
            b[4..8].copy_from_slice(&2u32.to_le_bytes());
            for (i, v) in [s_off, s_size, sensors.len(), r_off, r_size, readings.len()]
                .into_iter()
                .enumerate()
            {
                b[20 + 4 * i..24 + 4 * i].copy_from_slice(&(v as u32).to_le_bytes());
            }
            for (i, s) in sensors.iter().enumerate() {
                let at = s_off + i * s_size + 8;
                b[at..at + s.len()].copy_from_slice(s.as_bytes());
            }
            for (i, (kind, sensor, label, unit, value)) in readings.iter().enumerate() {
                let at = r_off + i * r_size;
                b[at..at + 4].copy_from_slice(&kind.to_le_bytes());
                b[at + 4..at + 8].copy_from_slice(&sensor.to_le_bytes());
                b[at + 12..at + 12 + label.len()].copy_from_slice(label.as_bytes());
                b[at + 268..at + 268 + unit.len()].copy_from_slice(unit);
                b[at + 284..at + 292].copy_from_slice(&value.to_le_bytes());
            }
            b
        }

        #[test]
        fn reads_temperatures_from_hwinfo() {
            let b = memory(
                &["CPU [#0]: AMD Ryzen 7 5800X", "S.M.A.R.T.: WD Blue"],
                &[
                    (TEMPERATURE, 0, "CPU (Tctl/Tdie)", b"\xb0C", 61.25),
                    (TEMPERATURE, 0, "Distance to TjMAX", b"\xb0C", 33.0),
                    (5, 0, "CPU Package Power", b"W", 88.0),
                    (TEMPERATURE, 1, "Drive Temperature", b"\xb0F", 104.0),
                ],
            );
            let r = parse(&b).unwrap();
            assert_eq!(r.len(), 3);
            assert_eq!(r[0].name, "CPU [#0]: AMD Ryzen 7 5800X: CPU (Tctl/Tdie)");
            assert_eq!(r[0].celsius, 61.25);
            assert_eq!(r[0].source, "hwinfo");
            assert_eq!(r[2].name, "S.M.A.R.T.: WD Blue: Drive Temperature");
            assert!((r[2].celsius - 40.0).abs() < 1e-4, "°F becomes °C");
            // The distance to TjMax is not a temperature.
            let real: Vec<_> = r
                .iter()
                .filter(|r| crate::win_temps::is_temperature(&r.name))
                .cloned()
                .collect();
            assert_eq!(crate::win_temps::hottest(&real), Some(61.25));
            assert_eq!(
                r[0].line(),
                "hwinfo    61.2 °C  CPU [#0]: AMD Ryzen 7 5800X: CPU (Tctl/Tdie)"
            );
        }

        /// The real path on Windows: a mapping named as HWiNFO names it
        /// (creating a `Global\` one needs administrator rights, which CI
        /// runners have; skipped otherwise, or while HWiNFO itself runs).
        #[cfg(windows)]
        #[test]
        fn reads_hwinfo_s_mapping() {
            use windows_sys::Win32::{
                Foundation::{CloseHandle, GetLastError, INVALID_HANDLE_VALUE},
                System::Memory::{
                    CreateFileMappingW, FILE_MAP_WRITE, MEMORY_MAPPED_VIEW_ADDRESS, MapViewOfFile,
                    PAGE_READWRITE, UnmapViewOfFile,
                },
            };
            let b = memory(
                &["CPU [#0]: Test CPU"],
                &[(TEMPERATURE, 0, "CPU Package", b"\xb0C", 57.5)],
            );
            let name: Vec<u16> = MAPPING.encode_utf16().chain([0]).collect();
            // SAFETY: a page-file mapping of `b.len()` bytes, written once
            // through a view that is unmapped before the read; the handle
            // stays open (so the mapping lives) until the end.
            unsafe {
                let h = CreateFileMappingW(
                    INVALID_HANDLE_VALUE,
                    std::ptr::null(),
                    PAGE_READWRITE,
                    0,
                    b.len() as u32,
                    name.as_ptr(),
                );
                if h.is_null() || GetLastError() == 183 {
                    eprintln!("skipped: cannot make {MAPPING} here (or HWiNFO runs)");
                    if !h.is_null() {
                        CloseHandle(h);
                    }
                    return;
                }
                let view = MapViewOfFile(h, FILE_MAP_WRITE, 0, 0, b.len());
                assert!(!view.Value.is_null());
                std::ptr::copy_nonoverlapping(b.as_ptr(), view.Value as *mut u8, b.len());
                UnmapViewOfFile(MEMORY_MAPPED_VIEW_ADDRESS { Value: view.Value });
                let r = read().expect("the mapping reads");
                CloseHandle(h);
                assert_eq!(r.len(), 1);
                assert_eq!(r[0].name, "CPU [#0]: Test CPU: CPU Package");
                assert_eq!(r[0].celsius, 57.5);
            }
            assert!(read().is_none(), "gone with its last handle");
        }

        #[test]
        fn refuses_what_is_not_hwinfo() {
            let mut b = memory(&["CPU"], &[(TEMPERATURE, 0, "Core", b"\xb0C", 50.0)]);
            assert!(parse(&b[..20]).is_err());
            let mut dead = b.clone();
            dead[0..4].copy_from_slice(&DEAD.to_le_bytes());
            assert!(
                parse(&dead)
                    .unwrap_err()
                    .to_string()
                    .contains("stopped sharing")
            );
            // A reading count that runs past the end.
            b[40..44].copy_from_slice(&9u32.to_le_bytes());
            assert!(parse(&b).unwrap_err().to_string().contains("run past"));
            b[40..44].copy_from_slice(&u32::MAX.to_le_bytes());
            assert!(parse(&b).unwrap_err().to_string().contains("damaged"));
            // Elements smaller than the layout.
            let mut small = memory(&[], &[(TEMPERATURE, 0, "Core", b"\xb0C", 50.0)]);
            small[36..40].copy_from_slice(&100u32.to_le_bytes());
            assert!(parse(&small).is_err());
            // A reading whose sensor index is out of range keeps its label.
            let b = memory(&[], &[(TEMPERATURE, 7, "Core", b"\xb0C", 50.0)]);
            assert_eq!(parse(&b).unwrap()[0].name, "Core");
        }
    }
}

/// The WMI sources, on the thread that opened them (COM objects stay on
/// theirs).
#[cfg(windows)]
pub struct Sources {
    acpi: Option<wmi::WMIConnection>,
    cimv2: Option<wmi::WMIConnection>,
    lhm: Option<wmi::WMIConnection>,
    ohm: Option<wmi::WMIConnection>,
    reads: u32,
}

#[cfg(windows)]
impl Sources {
    pub fn open() -> Self {
        let open = |ns: &str| {
            wmi::WMIConnection::with_namespace_path(ns)
                .map_err(|e| tracing::debug!("WMI {ns}: {e}"))
                .ok()
        };
        Self {
            acpi: open("root\\WMI"),
            cimv2: open("root\\cimv2"),
            lhm: None,
            ohm: None,
            reads: 0,
        }
    }

    /// Every temperature every source has now, distances to TjMax left
    /// out.
    pub fn read(&mut self) -> Vec<Reading> {
        // The monitors may start after the agent: look for them now and
        // then.
        if self.reads.is_multiple_of(30) {
            if self.lhm.is_none() {
                self.lhm =
                    wmi::WMIConnection::with_namespace_path("root\\LibreHardwareMonitor").ok();
            }
            if self.ohm.is_none() {
                self.ohm =
                    wmi::WMIConnection::with_namespace_path("root\\OpenHardwareMonitor").ok();
            }
        }
        self.reads += 1;
        let mut out = Vec::new();
        if let Some(c) = &self.acpi {
            out.extend(read_acpi(c));
        }
        if let Some(c) = &self.cimv2 {
            out.extend(read_zones(c));
        }
        for (c, source) in [(&self.lhm, "lhm"), (&self.ohm, "ohm")] {
            if let Some(c) = c {
                out.extend(read_monitor(c, source));
            }
        }
        out.extend(hwinfo::read().unwrap_or_default());
        out.retain(|r| is_temperature(&r.name));
        out
    }
}

#[cfg(windows)]
type Row = std::collections::HashMap<String, wmi::Variant>;

#[cfg(windows)]
fn number(v: Option<&wmi::Variant>) -> Option<f64> {
    use wmi::Variant;
    Some(match v? {
        Variant::R4(f) => *f as f64,
        Variant::R8(f) => *f,
        Variant::I4(n) => *n as f64,
        Variant::UI4(n) => *n as f64,
        Variant::I8(n) => *n as f64,
        Variant::UI8(n) => *n as f64,
        // Performance counters come as strings of digits.
        Variant::String(s) => s.trim().parse().ok()?,
        _ => return None,
    })
}

#[cfg(windows)]
fn string(v: Option<&wmi::Variant>) -> String {
    match v {
        Some(wmi::Variant::String(s)) => s.clone(),
        _ => String::new(),
    }
}

/// ACPI thermal zones, in tenths of a kelvin (needs administrator rights,
/// which the service has).
#[cfg(windows)]
fn read_acpi(c: &wmi::WMIConnection) -> Vec<Reading> {
    let rows: Vec<Row> = c
        .raw_query("SELECT InstanceName, CurrentTemperature FROM MSAcpi_ThermalZoneTemperature")
        .unwrap_or_default();
    rows.iter()
        .filter_map(|r| {
            Some(Reading {
                source: "acpi",
                name: string(r.get("InstanceName")),
                celsius: (number(r.get("CurrentTemperature"))? / 10.0 - 273.15) as f32,
            })
        })
        .collect()
}

/// The same zones through the "Thermal Zone Information" counters, which
/// any user may read (1.17): tenths of a kelvin where Windows has them,
/// whole kelvins otherwise.
#[cfg(windows)]
fn read_zones(c: &wmi::WMIConnection) -> Vec<Reading> {
    let rows: Vec<Row> = c
        .raw_query(
            "SELECT Name, Temperature, HighPrecisionTemperature FROM Win32_PerfFormattedData_Counters_ThermalZoneInformation",
        )
        .unwrap_or_default();
    rows.iter()
        .filter_map(|r| {
            let k = match number(r.get("HighPrecisionTemperature")) {
                Some(dk) if dk > 0.0 => dk / 10.0,
                _ => number(r.get("Temperature"))?,
            };
            Some(Reading {
                source: "zone",
                name: string(r.get("Name")),
                celsius: (k - 273.15) as f32,
            })
        })
        .collect()
}

/// LibreHardwareMonitor's or Open Hardware Monitor's temperature sensors
/// (CPU cores, GPUs, drives), while it runs.
#[cfg(windows)]
fn read_monitor(c: &wmi::WMIConnection, source: &'static str) -> Vec<Reading> {
    let rows: Vec<Row> = c
        .raw_query("SELECT Name, Parent, Value FROM Sensor WHERE SensorType = 'Temperature'")
        .unwrap_or_default();
    rows.iter()
        .filter_map(|r| {
            Some(Reading {
                source,
                name: format!("{} {}", string(r.get("Parent")), string(r.get("Name")))
                    .trim()
                    .to_string(),
                celsius: number(r.get("Value"))? as f32,
            })
        })
        .collect()
}

/// What every source reads now, for `pumasi sensors --raw` (Windows; empty
/// elsewhere).
pub fn raw() -> Vec<Reading> {
    #[cfg(windows)]
    {
        Sources::open().read()
    }
    #[cfg(not(windows))]
    {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distances_and_limits_are_not_temperatures() {
        for n in [
            "CPU Core #1 Distance to TjMax",
            "Core Max Distance to TjMax",
            "CPU [#0]: Intel Core i7-4790K: Distance to TjMAX",
            "Core #0 TjMax",
            "GPU Thermal Limit",
        ] {
            assert!(!is_temperature(n), "{n}");
        }
        for n in [
            "CPU Package",
            "/intelcpu/0 CPU Core #1",
            "CPU (Tctl/Tdie)",
            "\\_TZ.THRM_0",
            "GPU Core",
            "Drive Temperature",
        ] {
            assert!(is_temperature(n), "{n}");
        }
    }

    #[test]
    fn the_hottest_plausible_reading_wins() {
        let r = |c| Reading {
            source: "lhm",
            name: "x".into(),
            celsius: c,
        };
        assert_eq!(
            hottest(&[r(40.0), r(71.5), r(0.0), r(200.0), r(f32::NAN)]),
            Some(71.5)
        );
        assert_eq!(hottest(&[r(-273.15)]), None);
        assert_eq!(hottest(&[]), None);
    }
}
