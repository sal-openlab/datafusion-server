FROM rust:1.78 as builder

ENV PYTHON_VERSION=3.11.9
ENV PYTHON_HOME=/opt/python
ENV PYO3_PYTHON=$PYTHON_HOME/bin/python3
ENV LANG=C.UTF-8

WORKDIR /tmp/python

RUN curl -O https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz \
 && tar -xf Python-${PYTHON_VERSION}.tgz \
 && cd Python-${PYTHON_VERSION} \
 && ./configure \
      --prefix=$PYTHON_HOME \
      --enable-shared \
      --enable-option-checking=fatal \
      --enable-optimizations \
      --with-lto \
      --enable-ipv6 \
      --disable-test-modules \
      --with-ensurepip=upgrade \
 && make -j8 build_all \
 && make install \
 && make clean \
 && echo "$PYTHON_HOME/lib" > /etc/ld.so.conf.d/python.conf \
 && ldconfig \
 && PATH=$PYTHON_HOME/bin:$PATH \
 && pip3 install --no-cache-dir "requests==2.31.0" \
 && pip3 install --no-cache-dir "pandas[performance,excel,pyarrow]==2.2.2" \
 && pip3 install --no-cache-dir "smbprotocol[kerberos]==1.13.0" \
 && find $PYTHON_HOME -depth \
      \( \
          \( -type d -a \( -name test -o -name tests -o -name idle_test \) \) \
          -o \( -type f -a \( -name '*.pyc' -o -name '*.pyo' -o -name 'libpython*.a' \) \) \
      \) -exec rm -rf '{}' + \
 && python3 --version

WORKDIR /usr/src/datafusion-server

COPY ./lib ./lib
COPY ./bin ./bin
COPY ./Cargo.toml ./

RUN cd bin \
 && cargo build --release --features=plugin,flight,avro,webdav,deltalake

FROM debian:bookworm-slim

ENV PYTHON_HOME=/opt/python
ENV PYTHONIOENCODING=utf-8
ENV PATH=$PYTHON_HOME/bin:$PATH
ENV LANG=C.UTF-8

COPY --from=builder /usr/src/datafusion-server/bin/target/release/datafusion-server /usr/local/datafusion-server/bin/
COPY --from=builder $PYTHON_HOME/bin/ $PYTHON_HOME/bin/
COPY --from=builder $PYTHON_HOME/lib/ $PYTHON_HOME/lib/
COPY ./bin/data /var/datafusion-server/data
COPY ./bin/plugins /usr/local/datafusion-server/plugins
COPY ./bin/config-release.toml /etc/datafusion-server-config.toml

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libkrb5-dev dumb-init \
 && echo "$PYTHON_HOME/lib" > /etc/ld.so.conf.d/python.conf \
 && ldconfig \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/* /var/cache/apt/*

WORKDIR /

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/usr/local/datafusion-server/bin/datafusion-server", "-f", "/etc/datafusion-server-config.toml"]
