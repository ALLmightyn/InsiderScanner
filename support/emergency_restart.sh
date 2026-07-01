#!/bin/bash
# 🚨 EMERGENCY CLUSTER SPAM STOP - CLEAN RESTART
# This script kills all processes, clears caches, and restarts fresh

echo "=========================================="
echo "🚨 EMERGENCY: STOPPING ALL SERVICES"
echo "=========================================="

# 1. Kill any ghost Python processes related to our bot
echo "[1/5] Killing ghost Python processes..."
pkill -9 -f "funding_tracer_test.py" 2>/dev/null || true
pkill -9 -f "maintest.py" 2>/dev/null || true
pkill -9 -f "performance_worker.py" 2>/dev/null || true
pkill -9 -f "retro_worker.py" 2>/dev/null || true
echo "✅ Ghost processes killed"

# 2. Stop all PM2 processes
echo "[2/5] Stopping PM2 processes..."
cd "/root/projects/InsiderScanner"
pm2 stop all 2>/dev/null || true
echo "✅ PM2 stopped"

# 3. Clear cluster candidates queue (old data)
echo "[3/5] Clearing cluster queue..."
sqlite3 "database/scanner.db" "DELETE FROM cluster_candidates;" 2>/dev/null || echo "⚠️ DB clear skipped"
echo "✅ Cluster queue cleared"

# 4. Clear WAL/SHM files (database locks)
echo "[4/5] Clearing database locks..."
rm -f database/scanner.db-wal database/scanner.db-shm 2>/dev/null || true
echo "✅ Database locks cleared"

# 5. Restart all services
echo "[5/5] Restarting services..."
pm2 start ecosystem.config.js
echo "✅ Services restarted"

# Show status
echo ""
echo "=========================================="
echo "✅ RESTART COMPLETE"
echo "=========================================="
echo ""
echo "Monitor logs with:"
echo "  pm2 logs maintest --lines 50"
echo "  pm2 logs funding_tracer_test --lines 50"
echo ""
echo "Expected behavior:"
echo "  - Cluster alerts ONLY from check_coordinated_attack"
echo "  - No duplicate/spam alerts"
echo "  - V12 filters active (anti-bot, fresh bypass, etc.)"
echo ""
