FROM rust:1.59-bullseye AS builder
WORKDIR /app

COPY ["./springline", "."]
COPY ["./resources", "resources"]

# --- Build our project
ENV RUSTFLAGS "--cfg tokio_unstable"

RUN cargo build --release


FROM debian:bullseye-slim AS runtime

WORKDIR /app
RUN apt-get update \
#    # python is only needed for the access server client
#    && apt-get install -y python3.9 python3-pip \
#    && apt-get install -y curl \
     # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

#   python is only needed for the access server client
#RUN pip install requests

# --- Create appuser
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

#COPY ["./hak/hak", "/usr/local/bin/hak"]

#ENV RUST_LOG "info,tokio=trace,runtime=trace"
ENV RUST_LOG "info,springline::flink=debug,springline::kubernetes=debug,springline::phases::act=debug"
ENV RUST_BACKTRACE 1
ENV POLAR_LOG 1

ENTRYPOINT ["./springline"]
#CMD ["--env", "${APP_ENVIRONMENT}"]
#CMD ["-s", "/secrets/secret.yaml"]
