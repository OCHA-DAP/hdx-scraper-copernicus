FROM public.ecr.aws/unocha/python:3.13-stable

WORKDIR /srv

COPY .. .

RUN --mount=type=bind,source=requirements.txt,target=requirements.txt \
    apk add --no-cache --upgrade --virtual .build-deps \
        git && \
    aws-cli \
    gdal-driver-parquet \
    gdal-tools && \
    apk add --no-cache --virtual .build-deps \
    build-base \
    gdal-dev \
    llvm15-dev && \
    LLVM_CONFIG=/usr/bin/llvm-config-15 \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir . && \
    apk del .build-deps && \
    rm -rf /var/lib/apk/*

CMD "python3 run.py"
