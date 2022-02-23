FROM lukemathwalker/cargo-chef:latest-rust-1.58 AS chef
WORKDIR /app


FROM chef AS planner
COPY ["./springline", "."]
# Compute a lock-like file for our project
RUN cargo chef prepare --recipe-path recipe.json


FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json

# Build our project dependencies, not our application!
RUN cargo chef cook --release --recipe-path recipe.json
COPY ["./springline", "."]
COPY ["./resources", "resources"]
#ENV SQLX_OFFLINE true
# Build our project
ENV RUSTFLAGS "--cfg tokio_unstable"

RUN cargo build --release


FROM debian:bullseye-slim AS runtime

WORKDIR /app
RUN apt-get update \
#   python is only needed for the access server client
    && apt-get install -y python3.9 python3-pip \
#    && apt-get install -y curl \
    # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

RUN pip install requests


# Create appuser
ENV USER=appuser
ENV UID=10001

RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"

USER appuser:appuser

COPY --from=builder ["/app/target/release/springline", "springline"]

COPY ["./resources", "resources"]
ENV APP_ENVIRONMENT production

COPY ["./hak/hak", "/usr/local/bin/hak"]

#ENV RUST_LOG "info,tokio=trace,runtime=trace"
ENV RUST_LOG "info"
ENV RUST_BACKTRACE 1
ENV POLAR_LOG 1

ENV KUBECONFIG "/secrets/environment.kubeconfig"
ENV HA_CREDENTIALS "/secrets/credentials.properties"
ENV TILLER_NAMESPACE "here-olp-pipeline-management-dev"
ENV CREDENTIALS_PROPERTIES "IwojIENvcHlyaWdodCAoQykgMjAxOSBIRVJFIEdsb2JhbCBCLlYuIGFuZCBpdHMgYWZmaWxpYXRlKHMpLgojIEFsbCByaWdodHMgcmVzZXJ2ZWQuCiMKIyBUaGlzIHNvZnR3YXJlIGFuZCBvdGhlciBtYXRlcmlhbHMgY29udGFpbiBwcm9wcmlldGFyeSBpbmZvcm1hdGlvbgojIGNvbnRyb2xsZWQgYnkgSEVSRSBhbmQgYXJlIHByb3RlY3RlZCBieSBhcHBsaWNhYmxlIGNvcHlyaWdodCBsZWdpc2xhdGlvbi4KIyBBbnkgdXNlIGFuZCB1dGlsaXphdGlvbiBvZiB0aGlzIHNvZnR3YXJlIGFuZCBvdGhlciBtYXRlcmlhbHMgYW5kCiMgZGlzY2xvc3VyZSB0byBhbnkgdGhpcmQgcGFydGllcyBpcyBjb25kaXRpb25hbCB1cG9uIGhhdmluZyBhIHNlcGFyYXRlCiMgYWdyZWVtZW50IHdpdGggSEVSRSBmb3IgdGhlIGFjY2VzcywgdXNlLCB1dGlsaXphdGlvbiBvciBkaXNjbG9zdXJlIG9mIHRoaXMKIyBzb2Z0d2FyZS4gSW4gdGhlIGFic2VuY2Ugb2Ygc3VjaCBhZ3JlZW1lbnQsIHRoZSB1c2Ugb2YgdGhlIHNvZnR3YXJlIGlzIG5vdAojIGFsbG93ZWQuCiMKCmhlcmUudXNlci5pZCA9IEhFUkUtZjE2ZDVhYmYtNTA1Ny00NWRkLTkzMDMtMTJmOTM2OWRmYzgxCmhlcmUuY2xpZW50LmlkID0gdUpnZzdkUkJvMXlBM2hJTTdlOUMKaGVyZS5hY2Nlc3Mua2V5LmlkID0gZGREX1g5WlU3UXRwQTVKYjBiU3Z5UQpoZXJlLmFjY2Vzcy5rZXkuc2VjcmV0ID0gMS1uLU5WMjloNWduZkxzWHFyU19mSHZPZkpVVXFzRHAyckllcWtDZi1KMEt6YWNHVWtGQmUyU0taekNBVGZyZnprQ2MxbUNMeFU4MWhEaGczRFdYbGcKaGVyZS50b2tlbi5lbmRwb2ludC51cmwgPSBodHRwczovL3N0Zy5hY2NvdW50LmFwaS5oZXJlLmNvbS9vYXV0aDIvdG9rZW4KaGVyZS5jbGllbnQuZ3JvdXAgPSBzZHA6R1JPVVAtZGExZGQyODctMmY0OS00ZTk4LTkyYWMtNjlhZGNiZmQ2MWQ3Cg=="

ENTRYPOINT ["./springline"]
#CMD ["-s", "/secrets/secret.yaml"]
