FROM rust:1-bullseye AS builder
WORKDIR /app

COPY ["./springline", "."]
COPY ["./resources", "resources"]

# --- Build our project
ENV RUSTFLAGS "--cfg tokio_unstable"

RUN cargo build --bin springline --release


FROM debian:bullseye-slim AS runtime

WORKDIR /app
RUN apt-get update \
    && apt-get install -y curl vim \
     # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

COPY ["./resources", "resources"]
RUN chmod -v 666 ./resources/*

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


ENTRYPOINT ["./springline"]
#CMD "--search-path ${SETTINGS_SEARCH_PATH}"
#CMD ["--env", "${APP_ENVIRONMENT}"]
#CMD ["-s", "/secrets/secret.yaml"]
