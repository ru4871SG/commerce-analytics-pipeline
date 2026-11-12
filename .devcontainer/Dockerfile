FROM ghcr.io/astral-sh/uv:0.12.10@sha256:2bb3ebca0a796a155094a27773d290c4b074572e6107f171d88d086682fd2500 AS uv
FROM docker.io/library/eclipse-temurin:21.0.9_10-jre-noble@sha256:d3eb69add1874bc785382d6282db53a67841f602a1139dee6c4a1221d8c56568 AS java
FROM mcr.microsoft.com/devcontainers/base:3.0.12-ubuntu24.04@sha256:ec556aed93f62aa6d13fe72c5db09da4e29a7c9b87c568dbca0ee5717466926a

COPY --from=uv /uv /uvx /usr/local/bin/
COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH
