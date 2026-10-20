#!/bin/sh
set -eu

if ! command -v kubectl >/dev/null 2>&1; then
  echo "error: kubectl is not installed" >&2
  exit 1
fi

context="$(kubectl config current-context 2>/dev/null || true)"
if [ "$context" != "docker-desktop" ]; then
  echo "error: refusing context '${context:-<none>}'; expected docker-desktop" >&2
  exit 1
fi

server="$(kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.server}')"
case "$server" in
  https://kubernetes.docker.internal:* | https://127.0.0.1:* | https://localhost:*) ;;
  *)
    echo "error: refusing non-local Kubernetes API endpoint '$server'" >&2
    exit 1
    ;;
esac

kubectl --context docker-desktop --request-timeout=15s cluster-info >/dev/null
nodes="$(kubectl --context docker-desktop --request-timeout=15s get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')"
if [ "$nodes" != "docker-desktop" ]; then
  echo "error: refusing nodes '${nodes:-<none>}'; expected only docker-desktop" >&2
  exit 1
fi

echo "verified local Kubernetes context: $context ($server), node: $nodes"
