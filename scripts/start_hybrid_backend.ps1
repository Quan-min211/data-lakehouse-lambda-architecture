# =============================================================================
# start_hybrid_backend.ps1
# Khởi động cụm Backend Lakehouse ở máy nhà + mở Cloudflare Tunnel miễn phí
# để giao diện Web trên Vercel tự động chuyển từ HIBERNATING sang LIVE.
# =============================================================================

param(
    [int]$ApiPort = 8000
)

Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "  LAMBDA LAKEHOUSE · HYBRID HOME BACKEND LAUNCHER" -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan

if (!(Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "[INFO] Đã tạo file .env từ .env.example" -ForegroundColor Yellow
}

Write-Host "`n[1/3] Đang khởi động cụm Docker ở máy nhà..." -ForegroundColor Green
docker compose up -d

Write-Host "`n[2/3] Đang chờ FastAPI Serving Layer (http://localhost:$ApiPort/health)..." -ForegroundColor Green
$ready = $false
for ($i = 0; $i -lt 20; $i++) {
    try {
        $resp = Invoke-RestMethod -Uri "http://localhost:$ApiPort/health" -TimeoutSec 2
        if ($resp.status -eq "healthy") {
            $ready = $true
            break
        }
    } catch {}
    Start-Sleep -Seconds 2
}

if ($ready) {
    Write-Host "  -> FastAPI Serving Layer đã sẵn sàng!" -ForegroundColor Green
} else {
    Write-Host "  -> FastAPI đang khởi động nền, tiếp tục mở Tunnel..." -ForegroundColor Yellow
}

Write-Host "`n[3/3] Đang kiểm tra Cloudflare Tunnel (cloudflared)..." -ForegroundColor Green
if (Get-Command "cloudflared" -ErrorAction SilentlyContinue) {
    Write-Host "Copy đường link https://xxxx.trycloudflare.com bên dưới và dán vào ô 'Kết nối máy chủ (Serving API)' ở thanh bên trái trên trang Vercel của bạn:" -ForegroundColor Cyan
    cloudflared tunnel --url "http://localhost:$ApiPort"
} else {
    Write-Host "`n[CHƯA CÀI CLOUDFLARED]" -ForegroundColor Yellow
    Write-Host "Chạy lệnh sau trong PowerShell để cài Cloudflare Tunnel miễn phí:" -ForegroundColor White
    Write-Host "  winget install --id Cloudflare.cloudflared" -ForegroundColor Cyan
    Write-Host "Sau đó chạy lại script này để nhận link HTTPS public kết nối với Vercel!" -ForegroundColor White
}
