#!/usr/bin/env python3
"""Exercise real executable processes, including cancellation and SIGTERM draining."""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(url, body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    data = None if body is None else json.dumps(body).encode()
    return urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers), timeout=5)


def wait_for(fn):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            if fn():
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.02)
    raise AssertionError("timed out waiting for service state")


def run():
    out = ROOT / "results" / "smoke"
    out.mkdir(parents=True, exist_ok=True)
    sim_port, gateway_port = free_port(), free_port()
    sim_url, gateway_url = f"http://127.0.0.1:{sim_port}", f"http://127.0.0.1:{gateway_port}"
    config = {
        "listen": f"127.0.0.1:{gateway_port}", "model": "demo", "queue_size": 0,
        "health_interval": "20ms", "health_timeout": "100ms", "telemetry_ttl": "300ms",
        "circuit_cooldown": "100ms", "cancel_grace": "50ms", "failure_threshold": 1,
        "backends": [{"id": "smoke", "url": sim_url, "kind": "sim", "capacity_tokens": 4096, "max_concurrent": 2}],
    }
    config_path = out / "config.json"
    config_path.write_text(json.dumps(config))
    processes, logs, streams, checks = [], [], [], []
    try:
        env = dict(os.environ, SIM_ADMIN_TOKEN="smoke")
        for name, args in [
            ("simulator", ["-listen", f"127.0.0.1:{sim_port}", "-capacity", "4096", "-token-delay", "3ms"]),
            ("gateway", ["-config", str(config_path)]),
        ]:
            log = (out / f"{name}.log").open("w")
            logs.append(log)
            processes.append(subprocess.Popen([str(ROOT / "bin" / name), *args], env=env, stdout=log, stderr=log))

        def state():
            with request(gateway_url + "/readyz") as response:
                return json.load(response)

        wait_for(lambda: state()["backends"][0]["healthy"])
        for stream in (True, False):
            with request(gateway_url + "/v1/generate", {"prompt": "smoke test", "max_tokens": 3, "stream": stream}) as response:
                if stream:
                    chunks = [json.loads(line) for line in response]
                    assert len(chunks) == 4 and chunks[-1]["done"]
                else:
                    body = json.load(response)
                    assert body["done"] and body["response"] == "t0 t1 t2 "
        checks.append("streaming and non-streaming through executable gateway")

        for _ in range(2):
            response = request(gateway_url + "/v1/generate", {"prompt": "hold capacity", "max_tokens": 512})
            streams.append(response)
            assert json.loads(response.readline())["response"]
        try:
            request(gateway_url + "/v1/generate", {"prompt": "excess work", "max_tokens": 2})
            raise AssertionError("excess work should be rejected")
        except urllib.error.HTTPError as error:
            assert error.code == 429
            error.close()
        checks.append("overload returns HTTP 429 at the configured concurrency bound")
        for response in streams:
            response.close()
        streams.clear()
        wait_for(lambda: state()["backends"][0]["reserved_tokens"] == 0)
        with request(sim_url + "/health") as response:
            assert json.load(response)["used_tokens"] == 0
        checks.append("client disconnect cancels backend work and releases reservations")

        response = request(gateway_url + "/v1/generate", {"prompt": "shutdown during streaming", "max_tokens": 512})
        streams.append(response)
        assert json.loads(response.readline())["response"]
        processes[1].send_signal(signal.SIGTERM)
        response.read()
        assert processes[1].wait(timeout=7) == 0

        def backend_idle():
            with request(sim_url + "/health") as response:
                return json.load(response)["used_tokens"] == 0

        wait_for(backend_idle)
        checks.append("SIGTERM drains HTTP handling and cancels active generation")
        (out / "checks.json").write_text(json.dumps({"passed": checks}, indent=2) + "\n")
        for check in checks:
            print("PASS:", check)
    finally:
        for response in streams:
            response.close()
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=7)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for log in logs:
            log.close()


if __name__ == "__main__":
    run()
