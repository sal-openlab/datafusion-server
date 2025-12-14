#!/usr/bin/env bash
set -e

USER=${USER:-dev}
HOME=/home/$USER

if [ -n "${VNC_PASSWORD}" ]; then
  su - "$USER" -c "mkdir -p ~/.vnc && \
    printf '%s\n%s\n\n' \"$VNC_PASSWORD\" \"$VNC_PASSWORD\" | vncpasswd -f > ~/.vnc/passwd && \
    chmod 600 ~/.vnc/passwd"
fi

su - "$USER" -c "vncserver -kill :1 >/dev/null 2>&1 || true"
su - "$USER" -c "vncserver :1 \
  -geometry ${VNC_GEOMETRY:-1600x1000} \
  -depth ${VNC_DEPTH:-24} \
  -localhost no \
  -SecurityTypes VncAuth"

tail -F "$HOME"/.vnc/*.log
