# Kubernetes deployment

The root `kustomization.yaml` deploys the same simulator and monitoring stack as Docker Compose. Run commands from the repository root. You need a working Kubernetes cluster and kubectl configured for it. The local example also needs Docker and [kind](https://kind.sigs.k8s.io/docs/user/quick-start/).

## Deploy locally with kind

```sh
kind create cluster --name memory-gateway
docker build -t memory-inference-gateway:local .
kind load docker-image memory-inference-gateway:local --name memory-gateway

# Inspect the rendered resources without contacting a cluster.
make k8s-render

# Create the namespace and deploy all six workloads.
make k8s-apply
make k8s-smoke
```

The stack contains six Deployments and six ClusterIP Services in `memory-gateway`: `gateway`, `backend-a`, `backend-b`, `backend-c`, `prometheus`, and `grafana`. Startup, readiness, and liveness probes handle startup order; no Compose-style `depends_on` is needed. The gateway is ready when at least one backend is healthy and fresh. The smoke check waits for all three.

The image uses `IfNotPresent`, so kind must have the locally built image loaded before deployment. For another local runtime, use its image-loading mechanism. A remote cluster needs an image pushed to an accessible registry, as described below. The Go binaries are CPU-only simulators; this deployment needs no GPU plugin or GPU nodes.

## Access the gateway and monitoring

Run each port forward in a separate terminal:

```sh
kubectl -n memory-gateway port-forward --address=127.0.0.1 service/gateway 8080:8080
kubectl -n memory-gateway port-forward --address=127.0.0.1 service/prometheus 9090:9090
kubectl -n memory-gateway port-forward --address=127.0.0.1 service/grafana 3000:3000
```

```sh
curl http://127.0.0.1:8080/readyz
curl -N http://127.0.0.1:8080/v1/generate \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"Explain Kubernetes readiness","max_tokens":32}'
```

Prometheus is at <http://127.0.0.1:9090>. Grafana's provisioned dashboard is at <http://127.0.0.1:3000/d/memory-gateway>. Grafana permits anonymous Viewer access and disables initial admin creation; there is no default admin account in this deployment. These Services are reachable inside the cluster. Add authentication before exposing the gateway or monitoring beyond a trusted development environment.

Prometheus data and Grafana's database use `emptyDir` volumes and are lost when their Pods are replaced. Configuration and the dashboard are provisioned again on startup. Use persistent volumes if historical monitoring data must survive replacement.

## Configuration and updates

[Kustomize](https://kubernetes.io/docs/tasks/manage-kubernetes-objects/kustomization/) generates ConfigMaps directly from these existing files:

| Source | Use |
| --- | --- |
| `configs/compose.json` | Gateway listener, scheduling policy, and backend capacities/URLs |
| `monitoring/prometheus.yml` and `monitoring/alerts.yml` | Prometheus scrape configuration and rules |
| `monitoring/grafana/provisioning/` | Grafana data source and dashboard provider |
| `monitoring/grafana/dashboards/gateway.json` | Grafana dashboard |

Backend Service names preserve the configured URLs (`http://backend-a:9001`, etc.). The simulator arguments in `k8s/backends.yaml` match the configured capacities of 512, 1024, and 2048 tokens. Update both the simulator capacity and gateway configuration when changing these budgets.

After editing configuration, run `make k8s-apply` again. Generated ConfigMap names contain content hashes, so changes also update the corresponding Pod template and restart the affected workload. Old generated ConfigMaps can remain until namespace cleanup.

For a remote cluster, build and push an image for the cluster's node architecture. Add an `images` entry to the root `kustomization.yaml`, replacing the example registry and tag:

```yaml
images:
  - name: memory-inference-gateway
    newName: your-registry.example/team/memory-inference-gateway
    newTag: "v1"
```

```sh
docker build -t your-registry.example/team/memory-inference-gateway:v1 .
docker push your-registry.example/team/memory-inference-gateway:v1
make k8s-apply
make k8s-smoke
```

Use a new immutable tag for each build. Configure `imagePullSecrets` in the Pod templates if the registry requires authentication. A changed image tag triggers replacement. If rebuilding the development `:local` tag, reload it into kind, then run:

```sh
kubectl -n memory-gateway rollout restart deployment/backend-a deployment/backend-b deployment/backend-c deployment/gateway
make k8s-smoke
```

## Scheduling and scaling constraints

Keep **one gateway replica**. The scheduler's reservations, queue, and circuit state live in process memory. Multiple gateway processes sharing the same backends can overcommit their budgets. These manifests use `Recreate` so a normal Deployment update stops the old gateway before starting its replacement. Updates interrupt active generation and discard queued work; HTTP shutdown cancels upstream work and has a five-second drain timeout within the fifteen-second Pod termination grace period.

Keep **one replica per backend Service**. Each gateway backend entry represents one inference process and its telemetry. Load balancing a backend Service across multiple independent processes would detach telemetry and reservations from the process handling generation. To add capacity, create another single-replica backend Deployment/Service and add its distinct URL to the gateway configuration.

`Recreate` is an update strategy, not distributed coordination or fencing. Avoid manually launching a second gateway or running the Compose gateway against the same backends. Multi-replica gateway operation and autoscaling require further application changes. Resource requests and limits in these manifests are development defaults, separate from the gateway's logical token budgets.

The simulator fault endpoint is disabled because `SIM_ADMIN_TOKEN` is unset. To enable authenticated fault injection, create a Secret and reference it as that environment variable in the simulator containers. Simulator liveness uses a TCP probe so an intentional `/health` failure does not trigger a restart; readiness uses `/health` to remove the affected backend from Service endpoints until recovery.

## Validation and troubleshooting

`make k8s-smoke` needs Python 3 and kubectl. It checks rollout readiness, all three backend capacities through Service DNS, streaming and non-streaming generation, Prometheus metric collection and loaded alert rules, and Grafana dashboard/data source provisioning and connectivity. It uses temporary loopback port forwards and leaves the deployed workloads running. You can select another kubectl binary with `make KUBECTL=/path/to/kubectl k8s-smoke`.

CI creates a disposable kind cluster, loads the built image, validates the rendered objects against the API with a server dry run, applies them, and runs the same smoke check.

```sh
kubectl -n memory-gateway get pods,services
kubectl -n memory-gateway get events --sort-by=.lastTimestamp
kubectl -n memory-gateway describe pods
kubectl -n memory-gateway logs deployment/gateway
kubectl -n memory-gateway logs deployment/prometheus
kubectl -n memory-gateway logs deployment/grafana
```

For `ImagePullBackOff`, check the image name and that the image is loaded into each node or accessible from the registry. For gateway readiness failures, check the three backend Pods and `configs/compose.json`. For pending Pods, check available node resources. If a port forward exits after an update, start it again to attach to the replacement Pod.

## Cleanup

The following deletes the dedicated namespace and everything in it, including any data or extra resources you added there:

```sh
kubectl delete namespace memory-gateway
```

For the disposable local kind cluster, you can instead remove the entire cluster:

```sh
kind delete cluster --name memory-gateway
```
