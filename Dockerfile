FROM ubuntu:jammy

LABEL org.opencontainers.image.authors = "hello@axiom.co" 

RUN apt update; apt install -y ca-certificates

COPY artifacts/sentinelexport sentinelexport

ENV CONNECTION_STRING="" \
    STORAGE_URL="" \
    AXIOM_PERSONAL_TOKEN="" \
    AXIOM_ORG="" \
    AXIOM_DATASET_PREFIX="" \
    AXIOM_URL="https://api.axiom.co"

CMD ./sentinelexport export --axiom-url="${AXIOM_URL}" --axiom-personal-org="${AXIOM_ORG}" --storage-url="${STORAGE_URL}" --axiom-dataset-prefix="${AXIOM_DATASET_PREFIX}" 