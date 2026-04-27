# Reference Dockerfile for a pilot-langgraph worker.
#
# Build:
#   docker build -t pilot-langgraph-worker -f deploy/Dockerfile .
#
# Run (the pilot daemon must be reachable; either run it on the host and
# bind-mount /tmp/pilot.sock, or run it in a sidecar container):
#   docker run --rm --name pilot-worker \
#     -v /tmp/pilot.sock:/tmp/pilot.sock \
#     -v /var/lib/pilot-langgraph:/var/lib/pilot-langgraph \
#     -e PILOT_CHECKPOINT_DB=/var/lib/pilot-langgraph/checkpoints.db \
#     -p 9090:9090 \
#     pilot-langgraph-worker \
#     --handlers worker_handlers --port 5000 --metrics-port 9090 \
#     --log-format json --log-level INFO
#
# The image expects a `worker_handlers.py` (or any python module exposing
# either a `register(server)` function or `@pilot_handler` decorators) to
# be present in the working directory at runtime — bind-mount your handler
# code via `-v $(pwd)/handlers:/app/handlers` and pass `--handlers handlers`.
#
# Docker note: Pilot's NAT traversal works fine inside a container *as
# long as the daemon is on the host* (via the bind-mounted socket). Running
# the daemon itself inside Docker requires host networking or careful UDP
# port mapping — out of scope for this base image.

FROM python:3.13-slim

LABEL org.opencontainers.image.source="https://github.com/TeoSlayer/pilotprotocol"
LABEL org.opencontainers.image.description="pilot-langgraph worker (LangGraph extension over Pilot Protocol)"
LABEL org.opencontainers.image.licenses="AGPL-3.0-or-later"

# Run as a non-root user. The /tmp/pilot.sock bind-mount must be readable
# by this UID — if your host daemon runs as root, either chmod the socket
# or override `--user` on `docker run`.
RUN groupadd -r pilot && useradd -r -g pilot -u 1000 -m pilot

WORKDIR /app

# Copy the wheel out of dist/ — produced by `uv build` before docker build.
COPY dist/langgraph_pilot-*.whl /tmp/wheels/

RUN python -m pip install --no-cache-dir /tmp/wheels/langgraph_pilot-*.whl \
  && rm -rf /tmp/wheels

# Default to a non-existent handler module — users MUST override at runtime
# with their own handlers via -v $(pwd):/app and --handlers <module>.
ENV PILOT_SOCKET=/tmp/pilot.sock

USER pilot

EXPOSE 9090
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -fsS http://127.0.0.1:9090/healthz || exit 1

ENTRYPOINT ["python", "-m", "pilot_langgraph.worker"]
CMD ["--help"]
