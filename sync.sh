#!/bin/bash
echo "📦 Sincronizando con GitHub..."
git add .
git commit -m "Auto-sync: $(date '+%Y-%m-%d %H:%M:%S')"
git pull --rebase
git push
echo "✅ Sincronización completada"
