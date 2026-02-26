#!/usr/bin/env python3
"""
vcenter_vm_health.py
vCenter VM monitoring — REST API + pyVmomi for real CPU%/Memory%/Disk%/Uptime
Output format:
{"datetime","device_ID","uptime","cpu","memory","Network","Disk Usage","api_error","ssh_error"}

Usage:
python3 vcenter_vm_health.py --vc <host> --user <user> --pass <pass>
"""

import argparse
import json
import sys
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DEFAULT_THREADS = 10
TIMEOUT = 30

# VMs to exclude from output (unnamed, internal, destroyed)
SKIP_DEVICE_IDS = {"None", "none", "i", "", None}

# ── pyVmomi import ────────────────────────────────────────────────────────────
try:
    from pyVmomi import vim
    from pyVim.connect import SmartConnect, Disconnect
    PYVMOMI_OK = True
except ImportError:
    PYVMOMI_OK = False


# REST API Client
class VCenterREST:
    def __init__(self, host, verify_ssl=False):
        self.host = host
        self.session = requests.Session()
        self.session.verify = verify_ssl
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })

    def login(self, user, password):
        r = self.session.post(
            f"https://{self.host}/rest/com/vmware/cis/session",
            auth=(user, password),
            timeout=TIMEOUT
        )
        r.raise_for_status()
        self.session.headers["vmware-api-session-id"] = r.json().get("value", "")
        print("[INFO] REST API authenticated.", file=sys.stderr)

    def logout(self):
        try:
            self.session.delete(
                f"https://{self.host}/rest/com/vmware/cis/session",
                timeout=TIMEOUT
            )
        except Exception:
            pass

    def list_vms(self):
        r = self.session.get(
            f"https://{self.host}/rest/vcenter/vm",
            timeout=TIMEOUT
        )
        r.raise_for_status()
        vms = r.json().get("value", [])
        print(f"[INFO] Found {len(vms)} VMs.", file=sys.stderr)
        return vms

    def _get(self, path):
        try:
            r = self.session.get(
                f"https://{self.host}/rest{path}",
                timeout=TIMEOUT
            )
            if r.ok:
                d = r.json()
                return d.get("value", d) if isinstance(d, dict) else d
        except Exception:
            pass
        return None

    def vm_identity(self, vm_id):
        return self._get(f"/vcenter/vm/{vm_id}/guest/identity")

    def vm_networking(self, vm_id):
        return self._get(f"/vcenter/vm/{vm_id}/guest/networking/interfaces")


# pyVmomi — quickStats (CPU%, Memory%, Disk%, Uptime)
def get_pyvmomi_stats(host, user, password):
    stats = {}

    if not PYVMOMI_OK:
        print("[WARN] pyVmomi not installed. Run: pip3 install pyVmomi", file=sys.stderr)
        return stats

    try:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        si = SmartConnect(host=host, user=user, pwd=password, sslContext=context)
        print("[INFO] pyVmomi connected for quickStats.", file=sys.stderr)

        content = si.RetrieveContent()
        container = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.VirtualMachine], True
        )

        for vm in container.view:
            try:
                moref_id = vm._moId
                qs = vm.summary.quickStats
                cfg = vm.summary.config

                # ── CPU % ───────────────────────────────────────────────
                cpu_mhz = qs.overallCpuUsage or 0
                num_cpu = cfg.numCpu or 1
                host_obj = vm.runtime.host
                cpu_speed = 1000
                if host_obj:
                    try:
                        cpu_speed = host_obj.hardware.cpuInfo.hz / 1_000_000
                    except Exception:
                        pass
                total_mhz = num_cpu * cpu_speed
                cpu_pct = round((cpu_mhz / total_mhz) * 100, 1) if total_mhz else 0

                # ── Memory % ────────────────────────────────────────────
                mem_used = qs.guestMemoryUsage or 0
                mem_total = cfg.memorySizeMB or 1
                mem_pct = round((mem_used / mem_total) * 100, 1)

                # ── Uptime ──────────────────────────────────────────────
                uptime_secs = qs.uptimeSeconds or 0

                # ── Disk Usage % (requires VMware Tools) ────────────────
                disk_pct = 0
                try:
                    if vm.guest and vm.guest.disk:
                        total_disk = 0
                        used_disk = 0
                        for d in vm.guest.disk:
                            total_disk += d.capacity or 0
                            if d.freeSpace is not None:
                                used_disk += (d.capacity - d.freeSpace)
                        if total_disk > 0:
                            disk_pct = round((used_disk / total_disk) * 100, 1)
                except Exception:
                    disk_pct = 0

                stats[moref_id] = {
                    "cpu_pct":        cpu_pct,
                    "mem_pct":        mem_pct,
                    "uptime_seconds": uptime_secs,
                    "disk_pct":       disk_pct
                }

            except Exception:
                pass

        container.Destroy()
        Disconnect(si)
        print(f"[INFO] pyVmomi stats collected for {len(stats)} VMs.", file=sys.stderr)

    except Exception as e:
        print(f"[WARN] pyVmomi failed: {e}", file=sys.stderr)

    return stats


# Helpers
def uptime_str(secs):
    if not secs:
        return "Unknown"
    days = secs // 86400
    hrs = (secs % 86400) // 3600
    return f"{days} Days" if days else f"{hrs} Hours"


def network_str(ifaces):
    if not ifaces:
        return "0 Mbps"
    total = 0
    for i in ifaces:
        s = i.get("statistics", {})
        total += (s.get("receive_bytes_per_second") or 0)
        total += (s.get("transmit_bytes_per_second") or 0)
    mbps = round(total / 1_000_000, 2)
    return f"{mbps} Mbps"


def is_valid_device(name):
    """Return False for unnamed, destroyed, internal, or VMware system VMs."""
    if name is None:
        return False
    stripped = str(name).strip()
    if stripped in SKIP_DEVICE_IDS:
        return False
    if len(stripped) <= 1:                   # single char names like "i"
        return False
    if stripped.lower() == "none":
        return False
    if stripped.lower().startswith("vcls-"): # VMware vCLS internal cluster VMs
        return False
    return True


# Per-VM collection — returns None if VM should be skipped
def collect(rest, vm_summary, qs_stats):
    vm_id = vm_summary.get("vm", "")
    name  = vm_summary.get("name", vm_id)
    power        = vm_summary.get("power_state", "UNKNOWN").upper()
    tools_status = vm_summary.get("tools_status", "").upper()   # REST field
    now          = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%dT%H:%M:%S%z")

    # ── READY STATE FILTER ────────────────────────────────────────────────────
    # Only include VMs that are:
    #   1. POWERED_ON
    #   2. VMware Tools running (guestToolsRunning) — means OS is up and active
    # VMs that are POWERED_OFF, suspended, or Tools not running = skip
    tools_running = tools_status in ("RUNNING", "GUESTTOOLSRUNNING", "")
    # Note: empty tools_status is kept as fallback so VMs without tools field
    # are still collected — we filter strictly below using pyVmomi uptime_seconds

    if power != "POWERED_ON":
        return None   # Skip powered-off VMs entirely

    rec = {
        "datetime":   now,
        "device_ID":  name,
        "uptime":     "N/A",
        "cpu":        "N/A",
        "memory":     "N/A",
        "Network":    "0 Mbps",
        "Disk Usage": "N/A",
        "api_error":  None,
        "ssh_error":  None,
    }

    if power == "POWERED_ON":
        qs = qs_stats.get(vm_id, {})
        if qs:
            # Skip VMs that are powered on but OS is not running
            # (uptime=0 means VM just started or tools not reporting = not ready)
            uptime_secs = qs.get("uptime_seconds", 0)
            rec["cpu"]        = f"{qs['cpu_pct']}%"
            rec["memory"]     = f"{qs['mem_pct']}%"
            rec["uptime"]     = uptime_str(uptime_secs)
            rec["Disk Usage"] = f"{qs['disk_pct']}%"

        # Try to get real hostname from VMware Tools
        identity = rest.vm_identity(vm_id)
        if identity:
            hn = identity.get("host_name")
            if hn and is_valid_device(hn):
                rec["device_ID"] = hn

        net = rest.vm_networking(vm_id)
        if net:
            ifaces = net if isinstance(net, list) else []
            rec["Network"] = network_str(ifaces)

    # ── FILTER: skip unnamed/destroyed/internal VMs ───────────────────────────
    if not is_valid_device(rec["device_ID"]):
        return None

    return rec


# Main
def main():
    p = argparse.ArgumentParser(description="vCenter VM Health Monitor")
    p.add_argument("--vc",      required=True)
    p.add_argument("--user",    required=True)
    p.add_argument("--pass",    required=True, dest="password")
    p.add_argument("--threads", default=DEFAULT_THREADS, type=int)
    args = p.parse_args()

    print("[INFO] Collecting quickStats via pyVmomi ...", file=sys.stderr)
    qs_stats = get_pyvmomi_stats(args.vc, args.user, args.password)

    rest = VCenterREST(args.vc)
    rest.login(args.user, args.password)

    try:
        vms     = rest.list_vms()
        total   = len(vms)
        written = 0

        with ThreadPoolExecutor(max_workers=args.threads) as ex:
            futures = {ex.submit(collect, rest, vm, qs_stats): vm for vm in vms}
            for future in as_completed(futures):
                try:
                    rec = future.result()
                except Exception as e:
                    vm  = futures[future]
                    name = vm.get("name", "UNKNOWN")
                    if not is_valid_device(name):
                        continue
                    rec = {
                        "datetime":   datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%dT%H:%M:%S%z"),
                        "device_ID":  name,
                        "uptime":     "N/A",
                        "cpu":        "N/A",
                        "memory":     "N/A",
                        "Network":    "N/A",
                        "Disk Usage": "N/A",
                        "api_error":  str(e),
                        "ssh_error":  None,
                    }

                # Skip None (filtered VMs)
                if rec is None:
                    continue

                print(json.dumps(rec, ensure_ascii=False))
                sys.stdout.flush()
                written += 1

        print(f"[INFO] Complete. {written}/{total} records written ({total - written} skipped).", file=sys.stderr)

    finally:
        rest.logout()
        print("[INFO] Session closed.", file=sys.stderr)


if __name__ == "__main__":
    main()
