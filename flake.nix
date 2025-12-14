{
  description = "Rust + Python dev env for datafusion-server";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, fenix }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
        python = pkgs.python311;
        rustToolchain = fenix.packages.${system}.fromToolchainFile {
          file = ./rust-toolchain.toml;
          # Execute `nix run .#update-rust-toolchain` when rust-toolchain.toml changed.
          sha256 = "sha256-Qxt8XAuaUR2OMdKbN4u8dBJOhSHxS+uS06Wl9+flVEk=";
        };
        updateRustToolchain = pkgs.writeShellApplication {
          name = "update-rust-toolchain-sha";
          runtimeInputs = [ pkgs.nix pkgs.gnused pkgs.coreutils ];
          text = ''
            set -euo pipefail

            echo "[update] resetting sha256 to fakeSha256"
            sed -i.bak \
              -e '/rustToolchain = fenix\.packages\..*fromToolchainFile {/,/};/ { s#^[[:space:]]*sha256 = "sha256-[^"]*";#          sha256 = pkgs.lib.fakeSha256;# }' \
              flake.nix

            echo "[update] running \`nix develop\` to obtain the sha256"
            if nix develop 2>err.log; then
              echo "[update] unexpected success (sha256 already correct?)"
              rm -f err.log
              exit 0
            fi

            sha=$(grep -o 'got:[[:space:]]*sha256-[A-Za-z0-9+/=]*' err.log | sed 's/.*sha256-/sha256-/')
            if [ -z "$sha" ]; then
              echo "[update] failed to extract sha256" >&2
              cat err.log >&2
              exit 1
            fi

            echo "[update] updating 'flake.nix' with $sha"
            sed -i.bak \
              -e "/rustToolchain = fenix\\.packages\\..*fromToolchainFile {/,/};/ { s#^[[:space:]]*sha256 = pkgs\\.lib\\.fakeSha256;#          sha256 = \"$sha\";# }" \
              flake.nix

            rm -f err.log
            echo "[update] done. please review 'flake.nix' and commit the change."
          '';
        };      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            # Rust (pinned by rust-toolchain.toml via fenix)
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
        apps.update-rust-toolchain = {
          type = "app";
          program = "${updateRustToolchain}/bin/update-rust-toolchain-sha";
        };
      }
    );
}
