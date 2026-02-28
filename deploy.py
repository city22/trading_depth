#!/usr/bin/env python3
"""Deploy trading_depth to remote server via SSH/SFTP."""

import os
import paramiko
import time

HOST = "jp.city22.net"
PORT = 22
USER = "root"
PASS = "Madou123"
REMOTE_DIR = "/opt/trading_depth"

LOCAL_FILES = [
    "server.py",
    "exchange_manager.py",
    "requirements.txt",
    "static/index.html",
    "static/style.css",
    "static/app.js",
]

def run(ssh, cmd, check=True):
    print(f"  $ {cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=120)
    out = stdout.read().decode()
    err = stderr.read().decode()
    if out.strip():
        print(out.rstrip())
    if err.strip():
        print("STDERR:", err.rstrip())
    rc = stdout.channel.recv_exit_status()
    if check and rc != 0:
        raise RuntimeError(f"Command failed (rc={rc}): {cmd}")
    return out, err, rc

def main():
    print(f"Connecting to {HOST}...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, port=PORT, username=USER, password=PASS, timeout=30)
    print("Connected.\n")

    # Create dirs
    print("==> Setting up remote directories...")
    run(ssh, f"mkdir -p {REMOTE_DIR}/static")

    # Upload files
    print("\n==> Uploading files...")
    sftp = ssh.open_sftp()
    base = os.path.dirname(os.path.abspath(__file__))
    for rel in LOCAL_FILES:
        local_path = os.path.join(base, rel)
        remote_path = f"{REMOTE_DIR}/{rel}"
        print(f"  {rel} -> {remote_path}")
        sftp.put(local_path, remote_path)
    sftp.close()

    # Install Python + pip if needed
    print("\n==> Checking Python3...")
    run(ssh, "python3 --version")

    print("\n==> Installing dependencies (venv)...")
    run(ssh, f"apt-get install -y python3-venv python3-full -q 2>/dev/null || true", check=False)
    run(ssh, f"python3 -m venv {REMOTE_DIR}/venv")
    run(ssh, f"{REMOTE_DIR}/venv/bin/pip install -r {REMOTE_DIR}/requirements.txt -q")

    # Stop old instance if running
    print("\n==> Stopping any existing instance...")
    run(ssh, "pkill -f 'server.py' || true", check=False)
    run(ssh, "systemctl stop trading_depth 2>/dev/null || true", check=False)
    time.sleep(1)

    # Create systemd service
    print("\n==> Creating systemd service...")
    service = f"""[Unit]
Description=Trading Depth Monitor
After=network.target

[Service]
Type=simple
WorkingDirectory={REMOTE_DIR}
ExecStart={REMOTE_DIR}/venv/bin/python server.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
"""
    sftp = ssh.open_sftp()
    with sftp.open("/etc/systemd/system/trading_depth.service", "w") as f:
        f.write(service)
    sftp.close()

    run(ssh, "systemctl daemon-reload")
    run(ssh, "systemctl enable trading_depth")
    run(ssh, "systemctl restart trading_depth")

    time.sleep(3)

    print("\n==> Checking service status...")
    run(ssh, "systemctl status trading_depth --no-pager -l", check=False)

    # Verify port 8000 is listening
    print("\n==> Checking port 8000...")
    out, _, _ = run(ssh, "ss -tlnp | grep 8000 || echo 'NOT LISTENING'", check=False)

    # Check firewall
    print("\n==> Opening firewall port 8000 (if ufw/firewalld active)...")
    run(ssh, "ufw allow 8000/tcp 2>/dev/null || firewall-cmd --add-port=8000/tcp --permanent 2>/dev/null || true", check=False)
    run(ssh, "firewall-cmd --reload 2>/dev/null || true", check=False)

    ssh.close()
    print(f"\n✅ Deployment complete!")
    print(f"   Open: http://{HOST}:8000")

if __name__ == "__main__":
    main()
