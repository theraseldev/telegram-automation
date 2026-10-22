#!/bin/bash
echo ""
echo " ============================================"
echo "  TG Automation Bot - Desktop Launcher"
echo " ============================================"
echo ""

# Check Python
if ! command -v python3 &>/dev/null; then
    echo "[ERROR] python3 not found. Install Python 3.10+ first."
    exit 1
fi
echo "[1/3] Python3 found: $(python3 --version)"

# Install dependencies
echo "[2/3] Installing/checking requirements..."
python3 -m pip install -r requirements.txt -q --disable-pip-version-check

echo "[3/3] Starting server..."
echo ""
echo " Dashboard: http://localhost:5000"
echo " Login    : admin / admin123"
echo ""
echo " Press Ctrl+C to stop the server."
echo ""

# Open browser in background (works on Linux/Mac)
(sleep 3 && (xdg-open http://localhost:5000 2>/dev/null || open http://localhost:5000 2>/dev/null)) &

# Start Flask
python3 main.py
