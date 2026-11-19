# Future Implementation Blueprint: Remote Tunnel Access & Physical USB Hardware Key Gate

**Target System:** Auto-Broker Terminal (`v3.7 DEFCON 0`)  
**Phase:** Phase 8 — Remote Telemetry Tunneling + Hardware Token Gating

---

## 1. Architectural Overview

To allow remote monitoring of the **Autonomous Agent Heartbeat** and **Lie-Proof Ghost Portfolio** from a phone or external laptop while keeping physical capital (`FundGate`) and the `14141414` reinitialization protocol impenetrable, this blueprint combines:

1. **Option 1 Secure Tunneling (`ngrok` or `cloudflared`):** Exposes `http://127.0.0.1:8000` over an encrypted HTTPS tunnel without opening router ports.
2. **Physical USB Hardware Key Gate (`USB_KEY_PATH`):** Requires a physical USB flash drive containing a cryptographic key file (`trading_gate.key`) to be plugged into the host machine before any sensitive state changes (opening the Wallet Gate, switching to Live Mode, or unlocking after a Panic) can occur.

---

## 2. Two-Tier USB Gate Security Model

| Operation Type | Examples | When USB Key Is **INSERTED** | When USB Key Is **UNPLUGGED** |
| :--- | :--- | :--- | :--- |
| **Read-Only Telemetry** *(Optional Remote Token)* | Viewing Ghost Portfolio, Pulse Audit Log, RSI/SMA state | **ALLOWED** | **ALLOWED** *(with Basic Auth / Tunnel PIN)* or **403 BLOCKED** *(Strict Mode)* |
| **Emergency Panic Shunt** | Triggering `POST /api/panic` (`PANIC`) | **ALLOWED** | **ALLOWED** *(Can always pull the fire alarm remotely)* |
| **Capital & Recovery Gates** | Opening Wallet Gate (`FundGate`), Switching to `LIVE` mode, `14141414` Supervisor Reinitialization | **ALLOWED** *(SHA-256 signature on USB verified)* | **HARD BLOCKED (`403 USB_HARDWARE_KEY_MISSING`)** |

---

## 3. Setup Steps for Implementation

### Step 3.1: Provision the Physical USB Key
1. Insert your dedicated USB drive (e.g., mounted at `E:\`).
2. Generate a random 64-byte secret token and save it to `E:\trading_gate.key`:
   ```powershell
   $bytes = New-Object Byte[] 64
   [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
   $token = [BitConverter]::ToString($bytes).Replace('-', '')
   Set-Content -Path 'E:\trading_gate.key' -Value $token -NoNewline
   ```
3. Compute the SHA-256 hash of `E:\trading_gate.key` so the server only stores the hash in `.env` (never the raw token):
   ```powershell
   (Get-FileHash -Path 'E:\trading_gate.key' -Algorithm SHA256).Hash
   ```

### Step 3.2: Add USB Gate Variables to `.env`
```env
# Physical USB Hardware Gate Settings
USB_GATE_ENABLED=true
USB_GATE_STRICT_READ=false
USB_KEY_PATH=E:/trading_gate.key
USB_KEY_SHA256=PASTE_SHA256_HASH_OF_YOUR_USB_KEY_FILE_HERE

# Remote Tunnel Basic Auth (for viewing dashboard from phone over ngrok)
REMOTE_AUTH_USER=commander
REMOTE_AUTH_PASS=YOUR_STRONG_TUNNEL_PASSWORD
```

---

## 4. Drop-In Python Reference Implementation (`usb_gate.py`)

When ready to activate this blueprint, create `usb_gate.py` and hook `verify_usb_hardware_key()` into `server.py`:

```python
import hashlib
import os
from pathlib import Path


def verify_usb_hardware_key():
    """
    Verifies that the physical USB drive is mounted and that the trading_gate.key
    file matches the expected SHA-256 fingerprint in .env.
    """
    enabled = os.environ.get('USB_GATE_ENABLED', 'false').strip().lower() in ('1', 'true', 'yes')
    if not enabled:
        return {'ok': True, 'usb_present': True, 'mode': 'USB_GATE_BYPASSED'}

    key_path_str = os.environ.get('USB_KEY_PATH', 'E:/trading_gate.key').strip()
    expected_sha256 = os.environ.get('USB_KEY_SHA256', '').strip().upper()
    key_path = Path(key_path_str)

    if not key_path.exists() or not key_path.is_file():
        return {
            'ok': False,
            'usb_present': False,
            'reason': f'Physical USB hardware key not detected at {key_path_str}.',
        }

    try:
        raw_bytes = key_path.read_bytes().strip()
        actual_sha256 = hashlib.sha256(raw_bytes).hexdigest().upper()
        if expected_sha256 and actual_sha256 != expected_sha256:
            return {
                'ok': False,
                'usb_present': True,
                'reason': 'USB hardware key file signature mismatch (SHA-256 verification failed).',
            }
        return {'ok': True, 'usb_present': True, 'mode': 'USB_HARDWARE_VERIFIED'}
    except Exception as exc:
        return {'ok': False, 'usb_present': False, 'reason': f'USB read error: {exc}'}
```

---

## 5. Launching the Remote Tunnel (Option 1: `ngrok`)

1. Install ngrok on Windows:
   ```powershell
   winget install Ngrok.Ngrok
   ```
2. Start your local broker server:
   ```powershell
   & 'C:\Users\NicholasGrace\.local\bin\python3.14.exe' server.py
   ```
3. In a second PowerShell window, launch the HTTPS tunnel with built-in OAuth or Basic Auth protection:
   ```powershell
   ngrok http 8000 --basic-auth="commander:YOUR_STRONG_PASSWORD"
   ```
4. Open the `https://<random-id>.ngrok-free.app` URL on your phone from anywhere to check your **Active Investments (Gains & Losses)** and **Live Market Pulse & State Audit Log**.
