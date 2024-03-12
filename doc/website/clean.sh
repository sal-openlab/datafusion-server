#!/bin/sh

set -e

if [ $(basename $(pwd)) != 'website' ]; then
  echo "Must be executed in located at 'website' directory."
  exit 1
fi

rm -rf public/
rm -rf resources/
rm -rf themes/hugo-geekdoc/
rm -f .hugo_build.lock

echo "Successfully cleaned."
exit 0
