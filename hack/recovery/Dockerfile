# Local-only test image; build ./operator with CGO_ENABLED=0 first.
# Release images continue to use the repository's multi-stage Dockerfile.
FROM gcr.io/distroless/static:nonroot
COPY operator /operator
USER 65532:65532
ENTRYPOINT ["/operator", "manager"]
