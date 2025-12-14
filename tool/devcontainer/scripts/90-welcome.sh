# /etc/profile.d/90-welcome.sh

[[ $- != *i* ]] && return

human_bytes_from_k() {
  local v=$1 # KB
  local units=(K M G T)
  local i=0

  while [ $v -ge 1024 ] && [ $i -lt 3 ]; do
    v=$(( v / 1024 ))
    i=$(( i + 1 ))
  done

  printf "%d%s" "$v" "${units[$i]}"
}

style_print() {
  local text="$1"
  shift

  local seq=""
  local reset="$(tput sgr0)"
  local arg

  for arg in "$@"; do
    case "$arg" in
      # foreground
      black)   seq="${seq}$(tput setaf 0)" ;;
      red)     seq="${seq}$(tput setaf 1)" ;;
      green)   seq="${seq}$(tput setaf 2)" ;;
      yellow)  seq="${seq}$(tput setaf 3)" ;;
      blue)    seq="${seq}$(tput setaf 4)" ;;
      magenta) seq="${seq}$(tput setaf 5)" ;;
      cyan)    seq="${seq}$(tput setaf 6)" ;;
      white)   seq="${seq}$(tput setaf 7)" ;;
      # background
      bg-black)   seq="${seq}$(tput setab 0)" ;;
      bg-red)     seq="${seq}$(tput setab 1)" ;;
      bg-green)   seq="${seq}$(tput setab 2)" ;;
      bg-yellow)  seq="${seq}$(tput setab 3)" ;;
      bg-blue)    seq="${seq}$(tput setab 4)" ;;
      bg-magenta) seq="${seq}$(tput setab 5)" ;;
      bg-cyan)    seq="${seq}$(tput setab 6)" ;;
      bg-white)   seq="${seq}$(tput setab 7)" ;;
      # decoration
      bold)       seq="${seq}$(tput bold)" ;;
      underline)  seq="${seq}$(tput smul)" ;;
      reverse)    seq="${seq}$(tput rev)" ;;
      # other
      dim)        seq="${seq}$(tput dim 2>/dev/null || true)" ;;
      standout)   seq="${seq}$(tput smso 2>/dev/null || true)" ;;
      # ignoring other keyword
      *) ;;
    esac
  done

  printf "%s%s%s" "$seq" "$text" "$reset"
}

print_banner() {
  . /etc/os-release

  printf "\n%s %s\n" \
    "$(style_print "WELCOME TO HELIX + RUST DEV CONTAINER!" bold)" \
    "$(style_print "built at $(cat /etc/container-built)" dim)"

  printf "\n"
  printf "$NAME $VERSION ($(uname -o) $(uname -r) $(uname -m))\n"
  printf "\n"
  printf "  System information as of $(date)\n"
  printf "\n"

  free -k | {
    read
    read TITLE TOTAL USED FREE
    printf "  Memory usage: %d%% of %s\n" \
      $(( 100 * USED / TOTAL )) \
      "$(human_bytes_from_k "$TOTAL")"
    read TITLE TOTAL USED FREE
    printf "  Swap usage:   %d%% of %s\n" \
      $(( 100 * USED / TOTAL )) \
      "$(human_bytes_from_k "$TOTAL")"
  }
}

print_banner

python_version="(not found)"
if command -v python >/dev/null 2>&1; then
  python_version="$(python --version | cut -d ' ' -f 2 2>&1)"
elif command -v python3 >/dev/null 2>&1; then
  python_version="$(python3 --version | cut -d ' ' -f 2 2>&1)"
fi

node_version="(not found)"
if command -v node >/dev/null 2>&1; then
  node_version="$(node --version)"
fi

cat << EOF

Helix $(hx --version | cut -d ' ' -f 2) $(style_print "$(which hx)" dim)
Broot $(broot --version | cut -d ' ' -f 2) $(style_print "br -> $(which broot)" dim)
Lazygit $(lazygit --version | sed -n 's/.*version=\([^,]*\).*/\1/p; q') $(style_print "lg -> $(which lazygit)" dim)
Rust Toolchain $(cat /etc/default-rust-toolchain)
Python $python_version
Node.js $node_version

$(style_print "Note:" reverse) Do you need a different version of the rust toolchain?
  \$ rustup toolchain install x.yy
  \$ rustup component add rust-analyzer clippy rustfmt --toolchain x.yy

EOF
