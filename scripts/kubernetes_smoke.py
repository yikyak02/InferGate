#!/usr/bin/env python3
"""Check a deployed Kubernetes stack using temporary loopback port forwards."""
import argparse
from contextlib import contextmanager, ExitStack
import json
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


def request(url, body=None):
    data = None if body is None else json.dumps(body).encode()
    return urllib.request.urlopen(urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}), timeout=5)


def get_json(url):
    with request(url) as response:
        return json.load(response)


def wait_for(check, description, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.5)
    raise AssertionError("timed out waiting for " + description)


@contextmanager
def forward(kubectl, service, remote_port, health_path):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        local_port = sock.getsockname()[1]
    url = f"http://127.0.0.1:{local_port}"
    with tempfile.TemporaryFile(mode="w+") as log:
        process = subprocess.Popen([
            *kubectl, "port-forward", "--address=127.0.0.1",
            "service/" + service, f"{local_port}:{remote_port}"],
            stdout=log, stderr=log)
        try:
            def ready():
                if process.poll() is not None:
                    log.seek(0)
                    raise RuntimeError("port-forward failed: " + log.read())
                with request(url + health_path) as response:
                    return response.status == 200
            wait_for(ready, service + " port-forward", timeout=30)
            yield url
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def run():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubectl", default="kubectl")
    args = parser.parse_args()
    kubectl = [args.kubectl, "-n", "memory-gateway"]
    for name in ("backend-a", "backend-b", "backend-c", "gateway", "prometheus", "grafana"):
        subprocess.run([*kubectl, "rollout", "status", "deployment/" + name,
                        "--timeout=180s"], check=True)

    with ExitStack() as stack:
        gateway = stack.enter_context(forward(kubectl, "gateway", 8080, "/healthz"))
        prometheus = stack.enter_context(forward(kubectl, "prometheus", 9090, "/-/ready"))
        grafana = stack.enter_context(forward(kubectl, "grafana", 3000, "/api/health"))

        def all_backends_ready():
            backends = get_json(gateway + "/readyz")["backends"]
            expected = {"backend-a": 512, "backend-b": 1024, "backend-c": 2048}
            return (len(backends) == 3 and
                    {b["id"]: b["capacity_tokens"] for b in backends} == expected and
                    all(b["healthy"] and b["fresh"] and not b["circuit_open"] for b in backends))
        wait_for(all_backends_ready, "all three backends through Service DNS")
        print("PASS: gateway discovers all three backend Services and capacities")

        for stream in (True, False):
            with request(gateway + "/v1/generate", {
                    "prompt": "Kubernetes smoke test", "max_tokens": 3, "stream": stream}) as response:
                if stream:
                    chunks = [json.loads(line) for line in response]
                    assert len(chunks) == 4 and chunks[-1]["done"], chunks
                    assert "error" not in chunks[-1], chunks[-1]
                else:
                    body = json.load(response)
                    assert body["done"] and body["response"] == "t0 t1 t2 ", body
        print("PASS: streaming and non-streaming generation through the gateway Service")

        with request(gateway + "/metrics") as response:
            assert "gateway_backend_capacity_tokens" in response.read().decode()
        query = urllib.parse.urlencode({"query": 'gateway_backend_capacity_tokens{job="gateway"}'})

        def scraped():
            result = get_json(prometheus + "/api/v1/query?" + query)
            values = result["data"]["result"]
            return {v["metric"]["backend"]: int(float(v["value"][1])) for v in values} == {
                "backend-a": 512, "backend-b": 1024, "backend-c": 2048}
        wait_for(scraped, "Prometheus to scrape gateway metrics")
        rules = get_json(prometheus + "/api/v1/rules")["data"]["groups"]
        assert any(group["name"] == "inference-gateway" for group in rules), rules
        print("PASS: Prometheus scrapes gateway metrics and loads alert rules")

        dashboard = get_json(grafana + "/api/dashboards/uid/memory-gateway")
        assert dashboard["dashboard"]["uid"] == "memory-gateway", dashboard
        datasource = get_json(grafana + "/api/datasources/uid/prometheus")
        assert datasource["url"] == "http://prometheus:9090", datasource
        health = get_json(grafana + "/api/datasources/uid/prometheus/health")
        assert health["status"] == "OK", health
        print("PASS: Grafana provisions the dashboard and can query Prometheus")


if __name__ == "__main__":
    run()
