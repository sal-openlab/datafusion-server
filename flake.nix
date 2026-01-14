{
  description = "Rust + Python dev env for datafusion-server";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
        };
        python = pkgs.python311;
        rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            # Rust (pinned by rust-toolchain.toml via rust-overlay)
            rustToolchain
            pkgs.rust-analyzer
            pkgs.pkg-config

            # Python
            python
            pkgs.uv

            # Utilities used by shellHook
            pkgs.findutils
            pkgs.coreutils
          ];

          shellHook = ''
            set -euo pipefail

            export VIRTUAL_ENV="$PWD/.venv"
            if [ ! -d "$VIRTUAL_ENV" ]; then
              echo "[nix] creating python venv: $VIRTUAL_ENV"
              ${python.interpreter} -m venv "$VIRTUAL_ENV"
            fi
            export PATH="$VIRTUAL_ENV/bin:$PATH"

            # Prefer the venv python for PyO3 / plugins
            export PYO3_PYTHON="$VIRTUAL_ENV/bin/python"
            export PYTHONNOUSERSITE=1
            export PIP_DISABLE_PIP_VERSION_CHECK=1

            # Install deps into the single venv.
            # Priority:
            #  1) requirements.lock (fully pinned) -> uv pip sync
            #  2) requirements.txt at repo root    -> uv pip install -r
            #  3) plugins/**/requirements.txt      -> uv pip install -r (each)

            reqs="$(find bin/plugins -maxdepth 3 -name requirements.txt -type f 2>/dev/null || true)"
            if [ -f requirements.lock ]; then
              echo "[nix] syncing python deps from requirements.lock (uv pip sync)"
              uv pip sync requirements.lock
            elif [ -f requirements.txt ]; then
              echo "[nix] installing python deps from requirements.txt (uv pip install)"
              uv pip install -r requirements.txt
            elif [ -n "$reqs" ]; then
              echo "[nix] installing plugin requirements into .venv (uv pip install)"
              printf '%s\n' "$reqs" | while IFS= read -r f; do
                [ -z "$f" ] && continue
                echo "[nix] - $f"
                uv pip install -r "$f"
              done
            else
              echo "[nix] no requirements.lock / requirements.txt found; skipping python deps"
            fi

            echo "[nix] ready: rustc=$(rustc --version | head -n1), python=$(python --version)"
          '';
        };
      }
    );
}
