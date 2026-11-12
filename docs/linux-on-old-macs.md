# Linux on a 2013-2014 Mac

The Intel Macs of 2013 and 2014 stop at macOS 10.15, 11 or 12. Under
macOS they take WASM jobs and (from macOS 11) microVM jobs, but never
Docker jobs, and their GPUs are reachable only through Metal. The same
machine under Linux takes everything a Linux PC does:

| | macOS (last supported) | Linux |
| --- | --- | --- |
| WASM jobs | yes | yes |
| Docker jobs | no | yes |
| microVM jobs | macOS 11 and 12 only (Virtualization.framework) | yes (KVM: Firecracker or QEMU) |
| WebGPU jobs | Metal | Vulkan, where the GPU has a Vulkan driver (below) |
| Security updates | ended | current |

This guide replaces macOS. Keeping both (dual boot) works too, but a
machine that only lends itself to the cluster does not need it. The
steps are the usual ones for these models; check the result with the
commands at the end.

## Before you start

- **Back up** anything on the Mac you want to keep. The installer erases
  the disk.
- Note the model (Apple menu > About This Mac) and look up its GPU and
  Wi-Fi card in the table at the end.
- Plug in **Ethernet** (built in on iMac, Mac mini and Mac Pro; a
  Thunderbolt or USB adapter on laptops). The Broadcom Wi-Fi of these
  Macs has no driver in the installer, and Wake-on-LAN needs a wire
  anyway.
- A USB stick of 8 GB or more, and a wired USB keyboard for an iMac
  whose Bluetooth keyboard is not paired yet.
- To be able to go back: these Macs reinstall macOS over the internet
  (hold Option-Command-R at power on), with no disc or stick.

## 1. Make the installer

Download **Ubuntu Server 24.04 LTS** (or Ubuntu Desktop, or Debian 12:
the steps are the same) for `amd64`, and write it to the USB stick with
[balenaEtcher](https://etcher.balena.io), or from a terminal on the Mac:

```sh
diskutil list                               # find the stick, e.g. /dev/disk4
diskutil unmountDisk /dev/disk4
sudo dd if=ubuntu-24.04-live-server-amd64.iso of=/dev/rdisk4 bs=4m
```

## 2. Boot it and install

1. Shut the Mac down, plug in the stick, power on and hold **Option**
   until the boot picker shows. Choose **EFI Boot**.
2. Install with the defaults, onto the whole internal disk. An iMac with
   a Fusion Drive shows two disks (a small SSD and a large hard disk):
   put the system on the SSD and leave the hard disk for data.
3. Turn on **OpenSSH server** when the installer offers it, so that the
   Mac can run without a screen afterwards.
4. Reboot and take the stick out. If the Mac shows a folder with a
   question mark, hold Option at power on once and pick the disk; it
   remembers it afterwards.

## 3. After the first boot

```sh
sudo apt update && sudo apt full-upgrade -y
sudo apt install -y mbpfan            # fan control; without it Macs run hot or loud
sudo apt install -y bcmwl-kernel-source   # only if you need Wi-Fi (BCM4360)
```

For a laptop that should keep working with the lid closed, set
`HandleLidSwitch=ignore` and `HandleLidSwitchExternalPower=ignore` in
`/etc/systemd/logind.conf` and run `sudo systemctl restart
systemd-logind`.

## 4. Docker, microVMs and the agent

```sh
sudo apt install -y docker.io
ls -l /dev/kvm                        # must exist: every one of these Macs has VT-x
curl -fsSL https://github.com/syang0531/pumasi/releases/latest/download/install.sh | sudo sh
sudo pumasi vm setup                  # guest kernel and Firecracker
pumasi vm status                      # "firecracker usable" or "qemu usable"
sudo pumasi service install --ticket pumasijoin... -- \
  --allow-docker --allow-microvm --allow-webgpu --wake-on-lan
```

`pumasi status` on the control plane should now show the Mac with
`wasm, docker, microvm`. Jobs with `executor = "image"` run there in
Docker.

## 5. Sleep and Wake-on-LAN

A desktop Mac that lends itself can simply stay on. To let it sleep and
be woken for work:

```sh
sudo apt install -y ethtool
ip -br link                           # the wired card, e.g. enp3s0f0
sudo ethtool -s enp3s0f0 wol g
sudo ethtool enp3s0f0 | grep Wake-on  # "Wake-on: g"
```

Make it permanent with NetworkManager (`nmcli connection modify NAME
802-3-ethernet.wake-on-lan magic`) or, on Ubuntu Server, in the card's
entry of `/etc/netplan/*.yaml`:

```yaml
      wakeonlan: true
```

Then `sudo systemctl suspend` on the Mac and `pumasi node wake NAME`
from another machine: it should be back online in under a minute.

## 6. The GPU, for WebGPU jobs

```sh
sudo apt install -y mesa-vulkan-drivers vulkan-tools
vulkaninfo --summary | grep deviceName
pumasi gpu webgpu                     # the adapters wasm jobs with gpus = 1 can use
```

| Mac | GPU | Vulkan on Linux |
| --- | --- | --- |
| MacBook Air (Mid 2013, Early 2014), MacBook Pro 13" (Late 2013, Mid 2014), Mac mini (Late 2014), iMac 21.5" (Late 2013, Mid 2014) | Intel HD 5000, Iris 5100, Iris Pro 5200 (Haswell) | Mesa's `hasvk` driver, installed by the line above. It is not a conformant Vulkan driver on Haswell, so see what `pumasi gpu webgpu` says. |
| MacBook Pro 15" (Late 2013, Mid 2014) with GeForce GT 750M; iMac (Late 2013) with GT 750M, GT 755M, GTX 775M or 780M | NVIDIA Kepler | The open `nouveau` driver has no usable Vulkan here. NVIDIA's legacy driver does: `sudo apt install nvidia-driver-470`. |
| Mac Pro (Late 2013) with FirePro D300, D500 or D700; iMac 5K (Late 2014) with Radeon R9 M290X | AMD, Southern Islands family | Mesa's `radv`, but these cards start on the old `radeon` kernel driver, which has no Vulkan (`lspci -k` shows "Kernel driver in use: radeon"). Add `radeon.si_support=0 amdgpu.si_support=1` to `GRUB_CMDLINE_LINUX_DEFAULT` in `/etc/default/grub`, then `sudo update-grub` and reboot. |
| iMac 5K (Late 2014) with Radeon R9 M295X | AMD, newer family | Mesa's `radv` on the `amdgpu` kernel driver, with nothing to change. |

A GPU without a Vulkan driver still leaves the Mac useful for WASM,
Docker and microVM jobs; the agent then lists no WebGPU adapter.

## Check

```sh
pumasi status                         # the Mac: online, wasm, docker, microvm
pumasi run examples/python-wordcount/job-image.toml
pumasi job <ID>                       # tasks on the Mac say as=docker
pumasi node <the Mac>                 # sleep, Wake-on-LAN and its wired card
```

## Going back to macOS

Power on holding **Option-Command-R**, wait for the globe, erase the
disk in Disk Utility (APFS, GUID partition map) and choose Reinstall
macOS.
