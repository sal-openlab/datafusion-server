# /etc/profile.d/20-dev-config-init.sh

[[ $- != *i* ]] && return

# Helix
HX_CONFIG_DEST="$HOME/.config/helix"
HX_CONFIG_DEFAULT="/etc/helix-default"
mkdir -p "$HX_CONFIG_DEST"
for f in config.toml languages.toml; do
  if [ ! -f "$HX_CONFIG_DEST/$f" ] && [ -f "$HX_CONFIG_DEFAULT/$f" ]; then
    cp "$HX_CONFIG_DEFAULT/$f" "$HX_CONFIG_DEST/$f"
  fi
done

# Broot
BR_CONFIG_DEST="$HOME/.config/broot"
BR_CONFIG_DEFAULT="/etc/broot-default"
mkdir -p "$BR_CONFIG_DEST"
for f in conf.hjson verbs.hjson; do
  if [ ! -f "$BR_CONFIG_DEST/$f" ] && [ -f "$BR_CONFIG_DEFAULT/$f" ]; then
    cp "$BR_CONFIG_DEFAULT/$f" "$BR_CONFIG_DEST/$f"
  fi
done

if [ ! -f "$BR_CONFIG_DEST/launcher/bash/br" ]; then
  broot --install >/dev/null
fi

# Lazygit
LG_CONFIG_DEST="$HOME/.config/lazygit"
LG_CONFIG_DEFAULT="/etc/lazygit-default"
mkdir -p "$LG_CONFIG_DEST"
for f in config.yml; do
  if [ ! -f "$LG_CONFIG_DEST/$f" ] && [ -f "$LG_CONFIG_DEFAULT/$f" ]; then
    cp "$LG_CONFIG_DEFAULT/$f" "$LG_CONFIG_DEST/$f"
  fi
done
