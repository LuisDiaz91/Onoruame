#!/bin/bash
# Iniciar servidor virtual
Xvfb :99 -screen 0 1280x800x24 &
export DISPLAY=:99

# Iniciar VNC
x11vnc -display :99 -forever -nopw -listen localhost -xkb &
websockify --web /usr/share/novnc 6080 localhost:5900 &

# Ejecutar Onoruame
python run.py
