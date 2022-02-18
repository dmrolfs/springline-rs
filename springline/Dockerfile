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


#ENV RUST_LOG "info,tokio=trace,runtime=trace"
ENV RUST_LOG "info"
ENV RUST_BACKTRACE 1
ENV POLAR_LOG 1


ENTRYPOINT ["./springline"]
#CMD ["-s", "/secrets/secret.yaml"]
