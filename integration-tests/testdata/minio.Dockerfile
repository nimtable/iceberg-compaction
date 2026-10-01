FROM public.ecr.aws/docker/library/alpine:3.20

# Use the original test versions from MinIO's release artifacts. Their old
# container images no longer allow anonymous pulls from Quay or Docker Hub.
ADD --checksum=sha256:9d4c81b674ac861d2291e9edf17de6bd1f7c7a11bb3412bb674436d225a9490d https://github.com/minio/minio/releases/download/RELEASE.2024-03-07T00-43-48Z/minio.linux-amd64.RELEASE.2024-03-07T00-43-48Z /usr/bin/minio
ADD --checksum=sha256:5a9199be9abae92ea1b56949ec0566b6be7e1fd8560a54d099de8003c264126b https://github.com/minio/mc/releases/download/RELEASE.2024-03-07T00-31-49Z/mc.linux-amd64.RELEASE.2024-03-07T00-31-49Z /usr/bin/mc
RUN chmod 0755 /usr/bin/minio /usr/bin/mc

ENTRYPOINT ["/usr/bin/minio"]
