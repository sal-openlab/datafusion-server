#!/bin/bash
# makes datafusion-server container with and without plugin feature

CONTAINER_NAME="datafusion-server"
ARCHIVE_PATH=./

CONTAINER_VER=$(sed -En '
  /^\[package]/,/^$/{
    s/version[ \t]*=[ \t]*//p
  }' < ./bin/Cargo.toml
)
CONTAINER_VER=${CONTAINER_VER//\"/}

echo "Building $CONTAINER_NAME:$CONTAINER_VER"
docker build -t "$CONTAINER_NAME:$CONTAINER_VER" -f ./bin/Dockerfile .
if [ $? -gt 0 ]; then
  echo "Fail to build"
  exit 1
fi

if [ "$1" != "--no-archive" ]; then
  echo "Archiving $ARCHIVE_PATH$CONTAINER_NAME-$CONTAINER_VER.tar.gz"
  docker save "$CONTAINER_NAME:$CONTAINER_VER" | gzip -c > "$ARCHIVE_PATH$CONTAINER_NAME-$CONTAINER_VER.tar.gz"
  if [ $? -gt 0 ]; then
    echo "Fail to archive"
    exit 1
  fi
fi

echo "Building $CONTAINER_NAME-without-plugin:$CONTAINER_VER"
docker build -t "$CONTAINER_NAME-without-plugin:$CONTAINER_VER" -f ./bin/Dockerfile.without-plugin .
if [ $? -gt 0 ]; then
  echo "Fail to build"
  exit 1
fi

if [ "$1" != "--no-archive" ]; then
  echo "Archiving $ARCHIVE_PATH$CONTAINER_NAME-without-plugin-$CONTAINER_VER.tar.gz"
  docker save "$CONTAINER_NAME-without-plugin:$CONTAINER_VER" | gzip -c > "$ARCHIVE_PATH$CONTAINER_NAME-without-plugin-$CONTAINER_VER.tar.gz"
  if [ $? -gt 0 ]; then
    echo "Fail to archive"
    exit 1
  fi
fi

cat << EOF
Successfully built

You can be executed a container like follows.

$ docker load -i datafusion-server-$CONTAINER_VER.tar.gz
$ docker run --rm \\
    -p 4000:4000 \\
    -v ./data:/var/datafusion-server/data \\
    --name datafusion-server \\
    datafusion-server:$CONTAINER_VER
EOF

exit 0
