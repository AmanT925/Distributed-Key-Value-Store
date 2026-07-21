FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake g++ make git ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cmake -B build -DCMAKE_BUILD_TYPE=Release -DKVSTORE_BUILD_TESTS=OFF \
    && cmake --build build -j$(nproc) --target kv_node kv_cli bench_client

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /src/build/kv_node /src/build/kv_cli /src/build/bench_client /app/
COPY config/cluster.docker.conf /app/cluster.conf
RUN mkdir -p /app/data
ENTRYPOINT ["/app/kv_node"]
