//! A minimal `mkfs.ext4`: writes an empty ext4 filesystem into a sparse
//! file, so the microVM executor needs no e2fsprogs (there is none on
//! Windows, and macOS only has it through Homebrew).
//!
//! The layout is the plain one without `flex_bg`: every block group of
//! 32768 4 KiB blocks holds its block bitmap, inode bitmap and inode table,
//! and groups 0, 1 and powers of 3, 5 and 7 start with a copy of the
//! superblock and the group descriptor table (`sparse_super`). Features:
//! `ext_attr`, `filetype`, `extents`, `sparse_super`, `large_file`,
//! `dir_nlink` and `extra_isize` with 256-byte inodes. No journal and no
//! checksums: the disks are thrown away, or only read, after one VM. The
//! filesystem holds `/` and `/lost+found` and passes `e2fsck -f`.
//!
//! Only metadata blocks are written. Zeroed inode tables are valid without
//! `uninit_bg`, so a multi-gigabyte disk costs a few hundred kilobytes.

use std::{
    io::{Seek, SeekFrom, Write},
    path::Path,
};

use anyhow::{Context, Result, ensure};

pub const BLOCK: u64 = 4096;
pub const BLOCKS_PER_GROUP: u64 = 8 * BLOCK;
/// Bytes per group; disk sizes are rounded up to whole groups.
pub const GROUP_BYTES: u64 = BLOCKS_PER_GROUP * BLOCK;
/// One inode per 16 KiB, like `mke2fs`'s default.
const INODES_PER_GROUP: u32 = (GROUP_BYTES / 16384) as u32;
const INODE_SIZE: u64 = 256;
const ITABLE_BLOCKS: u64 = INODES_PER_GROUP as u64 * INODE_SIZE / BLOCK;
const DESC_SIZE: u64 = 32;
const FIRST_INO: u32 = 11;
const ROOT_INO: u32 = 2;
const LOST_FOUND_INO: u32 = 11;

const COMPAT_EXT_ATTR: u32 = 0x8;
const INCOMPAT_FILETYPE: u32 = 0x2;
const INCOMPAT_EXTENTS: u32 = 0x40;
const RO_COMPAT_SPARSE_SUPER: u32 = 0x1;
const RO_COMPAT_LARGE_FILE: u32 = 0x2;
const RO_COMPAT_DIR_NLINK: u32 = 0x20;
const RO_COMPAT_EXTRA_ISIZE: u32 = 0x40;
const EXTENTS_FL: u32 = 0x80000;
const EXTRA_ISIZE: u16 = 32;

/// Rounds `size` up to whole block groups (at least one).
pub fn rounded_size(size: u64) -> u64 {
    size.div_ceil(GROUP_BYTES).max(1) * GROUP_BYTES
}

/// Creates `path` as a sparse file of [`rounded_size`]`(size)` bytes
/// holding an empty ext4 filesystem labeled `label`.
pub fn mkfs(path: &Path, size: u64, label: &str) -> Result<()> {
    ensure!(label.len() <= 16, "ext4 label {label:?} is too long");
    let size = rounded_size(size);
    let groups = size / GROUP_BYTES;
    ensure!(
        groups * BLOCKS_PER_GROUP <= u32::MAX as u64,
        "disk too large ({size} bytes)"
    );
    let mut f = super::create_sparse(path, size)?;
    let fs = Layout::new(groups, label, path);
    fs.write(&mut f)
        .with_context(|| format!("write ext4 filesystem to {}", path.display()))?;
    f.flush()?;
    Ok(())
}

struct Layout {
    groups: u64,
    gdt_blocks: u64,
    label: [u8; 16],
    uuid: [u8; 16],
    hash_seed: [u8; 16],
    now: u32,
}

/// Per-group block numbers.
struct Group {
    start: u64,
    has_super: bool,
    block_bitmap: u64,
    inode_bitmap: u64,
    inode_table: u64,
    /// First block after the metadata.
    data: u64,
}

impl Layout {
    fn new(groups: u64, label: &str, path: &Path) -> Self {
        let mut l = [0u8; 16];
        l[..label.len()].copy_from_slice(label.as_bytes());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let mut h = blake3::Hasher::new();
        h.update(b"pumasi/ext4/v1");
        h.update(path.as_os_str().as_encoded_bytes());
        h.update(&now.to_le_bytes());
        h.update(&std::process::id().to_le_bytes());
        let seed = h.finalize();
        let mut uuid = [0u8; 16];
        uuid.copy_from_slice(&seed.as_bytes()[..16]);
        uuid[6] = (uuid[6] & 0x0f) | 0x40; // version 4
        uuid[8] = (uuid[8] & 0x3f) | 0x80; // RFC 4122 variant
        let mut hash_seed = [0u8; 16];
        hash_seed.copy_from_slice(&seed.as_bytes()[16..32]);
        Self {
            groups,
            gdt_blocks: (groups * DESC_SIZE).div_ceil(BLOCK),
            label: l,
            uuid,
            hash_seed,
            now: now as u32,
        }
    }

    fn group(&self, g: u64) -> Group {
        let start = g * BLOCKS_PER_GROUP;
        let has_super = has_super(g);
        let block_bitmap = start + if has_super { 1 + self.gdt_blocks } else { 0 };
        Group {
            start,
            has_super,
            block_bitmap,
            inode_bitmap: block_bitmap + 1,
            inode_table: block_bitmap + 2,
            data: block_bitmap + 2 + ITABLE_BLOCKS,
        }
    }

    fn write<W: Write + Seek>(&self, f: &mut W) -> Result<()> {
        let g0 = self.group(0);
        ensure!(
            g0.data + 2 <= BLOCKS_PER_GROUP,
            "too many groups for the descriptor table"
        );
        // Group 0 also holds the data blocks of / and /lost+found.
        let root_block = g0.data;
        let lf_block = g0.data + 1;

        let mut descs = Vec::with_capacity((self.gdt_blocks * BLOCK) as usize);
        let mut free_blocks_total = 0u64;
        let mut free_inodes_total = 0u64;
        for g in 0..self.groups {
            let grp = self.group(g);
            let mut used = grp.data - grp.start;
            let mut used_inodes = 0;
            let mut dirs = 0u16;
            if g == 0 {
                used += 2;
                used_inodes = FIRST_INO; // 1..=10 reserved, 11 is lost+found
                dirs = 2;
            }
            let free_blocks = BLOCKS_PER_GROUP - used;
            let free_inodes = INODES_PER_GROUP - used_inodes;
            free_blocks_total += free_blocks;
            free_inodes_total += free_inodes as u64;

            let mut bitmap = vec![0u8; BLOCK as usize];
            set_bits(&mut bitmap, 0, used as usize);
            put(f, grp.block_bitmap * BLOCK, &bitmap)?;
            let mut ibitmap = vec![0u8; BLOCK as usize];
            set_bits(&mut ibitmap, 0, used_inodes as usize);
            // Bits past the last inode of the group are padding, set to 1.
            set_bits(
                &mut ibitmap,
                INODES_PER_GROUP as usize,
                BLOCK as usize * 8 - INODES_PER_GROUP as usize,
            );
            put(f, grp.inode_bitmap * BLOCK, &ibitmap)?;

            let mut d = [0u8; DESC_SIZE as usize];
            le32(&mut d, 0, grp.block_bitmap as u32);
            le32(&mut d, 4, grp.inode_bitmap as u32);
            le32(&mut d, 8, grp.inode_table as u32);
            le16(&mut d, 12, free_blocks as u16);
            le16(&mut d, 14, free_inodes as u16);
            le16(&mut d, 16, dirs);
            descs.extend_from_slice(&d);
        }
        descs.resize((self.gdt_blocks * BLOCK) as usize, 0);

        for g in 0..self.groups {
            let grp = self.group(g);
            if !grp.has_super {
                continue;
            }
            let sb = self.superblock(g as u16, free_blocks_total, free_inodes_total);
            // The primary superblock sits 1024 bytes into block 0; backups
            // start their group's first block.
            let at = if g == 0 { 1024 } else { grp.start * BLOCK };
            put(f, at, &sb)?;
            put(f, (grp.start + 1) * BLOCK, &descs)?;
        }

        // Inodes 2 (/) and 11 (/lost+found), and their directory blocks.
        let root = dir_inode(0o040755, 3, root_block, self.now);
        put(f, inode_offset(&g0, ROOT_INO), &root)?;
        let lf = dir_inode(0o040700, 2, lf_block, self.now);
        put(f, inode_offset(&g0, LOST_FOUND_INO), &lf)?;
        let mut block = vec![0u8; BLOCK as usize];
        let mut at = dirent(&mut block, 0, ROOT_INO, ".", 12);
        at = dirent(&mut block, at, ROOT_INO, "..", 12);
        dirent(
            &mut block,
            at,
            LOST_FOUND_INO,
            "lost+found",
            BLOCK as usize - at,
        );
        put(f, root_block * BLOCK, &block)?;
        let mut block = vec![0u8; BLOCK as usize];
        let at = dirent(&mut block, 0, LOST_FOUND_INO, ".", 12);
        dirent(&mut block, at, ROOT_INO, "..", BLOCK as usize - at);
        put(f, lf_block * BLOCK, &block)?;
        Ok(())
    }

    fn superblock(&self, group_nr: u16, free_blocks: u64, free_inodes: u64) -> Vec<u8> {
        let mut s = vec![0u8; 1024];
        let blocks = self.groups * BLOCKS_PER_GROUP;
        le32(&mut s, 0, (self.groups * INODES_PER_GROUP as u64) as u32); // s_inodes_count
        le32(&mut s, 4, blocks as u32); // s_blocks_count_lo
        le32(&mut s, 12, free_blocks as u32); // s_free_blocks_count_lo
        le32(&mut s, 16, free_inodes as u32); // s_free_inodes_count
        le32(&mut s, 20, 0); // s_first_data_block
        le32(&mut s, 24, 2); // s_log_block_size: 1024 << 2
        le32(&mut s, 28, 2); // s_log_cluster_size
        le32(&mut s, 32, BLOCKS_PER_GROUP as u32);
        le32(&mut s, 36, BLOCKS_PER_GROUP as u32); // s_clusters_per_group
        le32(&mut s, 40, INODES_PER_GROUP);
        le32(&mut s, 48, self.now); // s_wtime
        le16(&mut s, 54, 0xffff); // s_max_mnt_count: no forced checks
        le16(&mut s, 56, 0xef53); // s_magic
        le16(&mut s, 58, 1); // s_state: clean
        le16(&mut s, 60, 1); // s_errors: continue
        le32(&mut s, 64, self.now); // s_lastcheck
        le32(&mut s, 76, 1); // s_rev_level: dynamic
        le32(&mut s, 84, FIRST_INO);
        le16(&mut s, 88, INODE_SIZE as u16);
        le16(&mut s, 90, group_nr); // s_block_group_nr
        le32(&mut s, 92, COMPAT_EXT_ATTR);
        le32(&mut s, 96, INCOMPAT_FILETYPE | INCOMPAT_EXTENTS);
        le32(
            &mut s,
            100,
            RO_COMPAT_SPARSE_SUPER
                | RO_COMPAT_LARGE_FILE
                | RO_COMPAT_DIR_NLINK
                | RO_COMPAT_EXTRA_ISIZE,
        );
        s[104..120].copy_from_slice(&self.uuid);
        s[120..136].copy_from_slice(&self.label);
        s[236..252].copy_from_slice(&self.hash_seed);
        s[252] = 1; // s_def_hash_version: half_md4
        le32(&mut s, 264, self.now); // s_mkfs_time
        le16(&mut s, 348, EXTRA_ISIZE); // s_min_extra_isize
        le16(&mut s, 350, EXTRA_ISIZE); // s_want_extra_isize
        // Directory hashes treat names as unsigned bytes, like mke2fs on
        // every architecture Linux boots this kernel on.
        le32(&mut s, 352, 0x2);
        s
    }
}

/// `sparse_super`: groups 0, 1 and powers of 3, 5 and 7 carry backups.
fn has_super(g: u64) -> bool {
    if g <= 1 {
        return true;
    }
    [3u64, 5, 7].iter().any(|&base| {
        let mut n = base;
        while n < g {
            n *= base;
        }
        n == g
    })
}

fn inode_offset(g0: &Group, ino: u32) -> u64 {
    g0.inode_table * BLOCK + (ino as u64 - 1) * INODE_SIZE
}

/// A directory inode of one block, mapped by a single extent.
fn dir_inode(mode: u16, links: u16, block: u64, now: u32) -> Vec<u8> {
    let mut i = vec![0u8; INODE_SIZE as usize];
    le16(&mut i, 0, mode);
    le32(&mut i, 4, BLOCK as u32); // i_size_lo
    le32(&mut i, 8, now); // i_atime
    le32(&mut i, 12, now); // i_ctime
    le32(&mut i, 16, now); // i_mtime
    le16(&mut i, 26, links);
    le32(&mut i, 28, (BLOCK / 512) as u32); // i_blocks_lo, in sectors
    le32(&mut i, 32, EXTENTS_FL);
    // i_block: extent header, then one extent.
    le16(&mut i, 40, 0xf30a); // eh_magic
    le16(&mut i, 42, 1); // eh_entries
    le16(&mut i, 44, 4); // eh_max
    le16(&mut i, 46, 0); // eh_depth
    le32(&mut i, 52, 0); // ee_block
    le16(&mut i, 56, 1); // ee_len
    le16(&mut i, 58, (block >> 32) as u16); // ee_start_hi
    le32(&mut i, 60, block as u32); // ee_start_lo
    le16(&mut i, 128, EXTRA_ISIZE);
    le32(&mut i, 144, now); // i_crtime
    i
}

/// Writes one `ext4_dir_entry_2` at `at`; returns the next offset.
fn dirent(block: &mut [u8], at: usize, ino: u32, name: &str, rec_len: usize) -> usize {
    le32(block, at, ino);
    le16(block, at + 4, rec_len as u16);
    block[at + 6] = name.len() as u8;
    block[at + 7] = 2; // EXT4_FT_DIR
    block[at + 8..at + 8 + name.len()].copy_from_slice(name.as_bytes());
    at + rec_len
}

fn set_bits(map: &mut [u8], from: usize, count: usize) {
    for bit in from..from + count {
        map[bit / 8] |= 1 << (bit % 8);
    }
}

fn le16(b: &mut [u8], at: usize, v: u16) {
    b[at..at + 2].copy_from_slice(&v.to_le_bytes());
}

fn le32(b: &mut [u8], at: usize, v: u32) {
    b[at..at + 4].copy_from_slice(&v.to_le_bytes());
}

fn put<W: Write + Seek>(f: &mut W, at: u64, data: &[u8]) -> Result<()> {
    f.seek(SeekFrom::Start(at))?;
    f.write_all(data)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_super_groups() {
        let with: Vec<u64> = (0..100).filter(|&g| has_super(g)).collect();
        assert_eq!(with, [0, 1, 3, 5, 7, 9, 25, 27, 49, 81]);
    }

    #[test]
    fn label_and_magic_where_the_guest_looks() {
        let dir = crate::tempdir::TempDir::new("ext4").unwrap();
        let path = dir.path().join("t.ext4");
        mkfs(&path, 1, "pumasi-scratch").unwrap();
        let len = std::fs::metadata(&path).unwrap().len();
        assert_eq!(len, GROUP_BYTES);
        let head = std::fs::read(&path).unwrap();
        assert_eq!(head[1080..1082], [0x53, 0xef]);
        assert_eq!(&head[1144..1158], b"pumasi-scratch");
    }

    /// `e2fsck -fn` finds nothing to fix, on a one-group disk and on one
    /// big enough for several backup superblocks and a two-block group
    /// descriptor table. Skipped where e2fsprogs is not installed.
    #[test]
    fn e2fsck_accepts_it() {
        let Some(fsck) = super::super::vmm::find_program("e2fsck", None, &["/sbin", "/usr/sbin"])
        else {
            eprintln!("e2fsck not found; skipping");
            return;
        };
        let dir = crate::tempdir::TempDir::new("ext4").unwrap();
        for (i, size) in [GROUP_BYTES, 10 << 30, 129 * GROUP_BYTES]
            .into_iter()
            .enumerate()
        {
            let path = dir.path().join(format!("t{i}.ext4"));
            mkfs(&path, size, "pumasi-base").unwrap();
            let out = std::process::Command::new(&fsck)
                .arg("-fn")
                .arg(&path)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "e2fsck on {size} bytes: {}\n{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            // Files and directories added afterwards keep it consistent.
            if let Some(debugfs) =
                super::super::vmm::find_program("debugfs", None, &["/sbin", "/usr/sbin"])
            {
                let src = dir.path().join("payload");
                std::fs::write(&src, vec![7u8; 3 << 20]).unwrap();
                let script = format!(
                    "mkdir a\nmkdir a/b\nwrite {} a/b/f\nsymlink l /a/b/f\n",
                    src.display()
                );
                let cmds = dir.path().join("cmds");
                std::fs::write(&cmds, script).unwrap();
                let out = std::process::Command::new(&debugfs)
                    .arg("-w")
                    .arg("-f")
                    .arg(&cmds)
                    .arg(&path)
                    .output()
                    .unwrap();
                assert!(
                    out.status.success(),
                    "{}",
                    String::from_utf8_lossy(&out.stderr)
                );
                let out = std::process::Command::new(&fsck)
                    .arg("-fn")
                    .arg(&path)
                    .output()
                    .unwrap();
                assert!(
                    out.status.success(),
                    "e2fsck after writes: {}",
                    String::from_utf8_lossy(&out.stdout)
                );
            }
        }
    }
}
