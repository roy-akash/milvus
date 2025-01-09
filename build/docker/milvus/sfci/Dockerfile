FROM docker.repo.local.sfdc.net/sfci/docker-images/sfdc_rhel9:70

LABEL maintainer=cdpfabric@salesforce.com

ARG TARGETARCH

RUN dnf install -y wget libgomp libaio libatomic ca-certificates openblas-devel libaio-devel && \
    dnf -y clean all && \
    rm -rf /var/cache/dnf/*

RUN yum install atlas-devel -y

COPY --chown=root:root --chmod=774 ./bin/ /milvus/bin/

COPY --chown=root:root --chmod=774 ./configs/ /milvus/configs/

COPY --chown=root:root --chmod=774 ./lib/ /milvus/lib/

ENV PATH=/milvus/bin:$PATH
ENV LD_LIBRARY_PATH=/milvus/lib:$LD_LIBRARY_PATH:/usr/lib
ENV LD_PRELOAD=/milvus/lib/libjemalloc.so
ENV MALLOC_CONF=background_thread:true

# Add Tini
ADD https://github.com/krallin/tini/releases/download/v0.19.0/tini-$TARGETARCH /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

WORKDIR /milvus
