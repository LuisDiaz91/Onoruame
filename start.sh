#!/bin/bash
echo "🚀 Iniciando PostgreSQL..."
docker start onoruame-db 2>/dev/null || docker run --name onoruame-db -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=onoruame -p 5432:5432 -d postgres:15

echo "🖥️  Iniciando servicios gráficos..."
pkill Xvfb 2>/dev/null
pkill x11vnc 2>/dev/null
pkill websockify 2>/dev/null

Xvfb :99 -screen 0 1280x800x24 &
sleep 2
export DISPLAY=:99
x11vnc -display :99 -forever -nopw -rfbport 5901 -listen 0.0.0.0 -xkb &
sleep 2
websockify --web /usr/share/novnc 0.0.0.0:6080 localhost:5901 &
sleep 2

echo "✅ Servicios listos"
echo "🌐 Abre en tu navegador: https://$CODESPACE_NAME-6080.app.github.dev/vnc.html"
echo "🚗 Iniciando Onoruame..."
export DISPLAY=:99
python main.py
