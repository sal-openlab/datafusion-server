FROM debian:bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8 \
    TZ=Asia/Tokyo

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    locales \
    tzdata \
    sudo \
    ca-certificates \
    curl \
    dumb-init \
    # desktop environment
    xterm lxqt-core openbox \
    libayatana-appindicator3-1 libayatana-indicator3-7 \
    dbus-x11 x11-xserver-utils fonts-dejavu fonts-noto-cjk \
    lxqt-themes adwaita-icon-theme hicolor-icon-theme \
    tigervnc-standalone-server tigervnc-common tigervnc-tools \
    firefox-esr \
    # utilities
    vim \
    less \
 && rm -rf /var/lib/apt/lists/* \
 && sed -i 's/^# *en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen \
 && locale-gen \
 && ln -sf /usr/share/zoneinfo/Asia/Tokyo /etc/localtime

# dev user
ENV USER=dev \
    HOME=/home/dev

RUN useradd -m -s /bin/bash "$USER" \
 && echo "$USER ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/$USER \
 && chmod 0440 /etc/sudoers.d/$USER \
 && mkdir -p $HOME/.config/lxqt \
 && printf '[General]\nicon_theme=Adwaita\n' > $HOME/.config/lxqt/lxqt.conf \
 && chown -R $USER:$USER $HOME/.config

# JetBrains Toolbox (optional)
ARG TOOLBOX_VERSION="3.1.2.64642"
ENV TOOLBOX_ROOT=/opt/jetbrains-toolbox

RUN ARCH="${TARGETARCH:-$(dpkg --print-architecture)}" \
 && case "$ARCH" in \
      amd64) JB_ARCH="" ;; \
      arm64) JB_ARCH="-arm64" ;; \
      *) echo "Unsupported arch for Jetbrains Toolbox: $ARCH" && exit 1 ;; \
    esac \
 && mkdir -p "$TOOLBOX_ROOT" \
 && curl -fsSL "https://download.jetbrains.com/toolbox/jetbrains-toolbox-${TOOLBOX_VERSION}${JB_ARCH}.tar.gz" -o /tmp/jetbrains-toolbox.tar.gz \
 && tar -xzf /tmp/jetbrains-toolbox.tar.gz -C "$TOOLBOX_ROOT" --strip-components=1 \
 && rm /tmp/jetbrains-toolbox.tar.gz \
 && ln -sf "$TOOLBOX_ROOT/bin/jetbrains-toolbox" /usr/local/bin/jetbrains-toolbox \
 && chown -R "$USER:$USER" "$TOOLBOX_ROOT"

RUN mkdir -p /usr/share/applications \
 && printf '[Desktop Entry]\n\
Type=Application\n\
Name=JetBrains Toolbox\n\
Comment=JetBrains Toolbox\n\
Exec=jetbrains-toolbox\n\
Icon=jetbrains-toolbox\n\
Terminal=false\n\
Categories=Development;IDE;\n\
StartupNotify=false\n' > /usr/share/applications/jetbrains-toolbox.desktop

# for VNC
RUN mkdir -p $HOME/.vnc \
 && cat << 'EOF' > $HOME/.vnc/xstartup
#!/bin/sh
unset SESSION_MANAGER
unset DBUS_SESSION_BUS_ADDRESS
export XKL_XMODMAP_DISABLE=1

export XDG_CURRENT_DESKTOP=LXQt
export XDG_SESSION_DESKTOP=lxqt
export XDG_SESSION_TYPE=x11

[ -r "$HOME/.Xresources" ] && xrdb "$HOME/.Xresources"

{
  echo "xstartup (lxqt) starting at $(date)"
  echo "DISPLAY=${DISPLAY:-unset}"
} >> "$HOME/.vnc/xstartup.log" 2>&1

export DISPLAY=${DISPLAY:-:1}

if command -v startlxqt >/dev/null 2>&1; then
  exec dbus-launch --exit-with-session startlxqt >> "$HOME/.vnc/xstartup.log" 2>&1
else
  exec /usr/bin/xterm -geometry 100x30+10+10 -ls -title "VNC xterm (fallback)" >> "$HOME/.vnc/xstartup.log" 2>&1
fi
EOF
RUN chmod +x $HOME/.vnc/xstartup \
 && chown -R $USER:$USER $HOME/.vnc

ENV VNC_GEOMETRY=1600x1000 \
    VNC_DEPTH=24 \
    VNC_PORT=5901

COPY --chmod=0755 entrypoint.sh /usr/local/bin/entrypoint.sh

EXPOSE 5901
USER root

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/usr/local/bin/entrypoint.sh"]
