#!/bin/sh

set -e

if [ -z $(which hugo) ]; then
  echo "Hugo CLI 'hugo' has not instaled, install 'hugo' first."
  exit 1
fi

if [ $(basename $(pwd)) != 'website' ]; then
  echo "Must be executed in located at 'website' directory."
  exit 1
fi

if [ -d "themes/hugo-geekdoc" ]; then
  echo "Already installed 'hugo-geekdoc' theme. \`rm -rf themes/hugo-geekdoc\` and re-run this script."
  exit 1
fi

mkdir -p themes/hugo-geekdoc/
curl -L https://github.com/thegeeklab/hugo-geekdoc/releases/latest/download/hugo-geekdoc.tar.gz | tar -xz -C themes/hugo-geekdoc/ --strip-components=1 

echo "Successfully prepared."
exit 0
