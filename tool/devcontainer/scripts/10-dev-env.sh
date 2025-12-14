# /etc/profile.d/10-dev-env.sh

export PYENV_ROOT="${PYENV_ROOT:-$HOME/.pyenv}"
export RUSTUP_HOME="${RUSTUP_HOME:-$HOME/.rustup}"
export CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"

export PATH="$PYENV_ROOT/shims:$PYENV_ROOT/bin:$CARGO_HOME/bin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

export HELIX_RUNTIME=/opt/helix/runtime
