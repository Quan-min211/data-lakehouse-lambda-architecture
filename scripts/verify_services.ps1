#!/usr/bin/env pwsh
# ============================================================
# verify_services.ps1
# Chay: .\scripts\verify_services.ps1
# Kiem tra toan bo Docker services sau khi docker compose up -d
# ============================================================

$services = @(
    @{ name = "ClickHouse";    url = "http://localhost:8123/ping";        expected = "Ok" },
    @{ name = "MinIO Health";  url = "http://localhost:9000/minio/health/live"; expected = "200" },
    @{ name = "Iceberg REST";  url = "http://localhost:8181/v1/config";   expected = "200" },
    @{ name = "FastAPI";       url = "http://localhost:8000/health";      expected = "healthy" },
    @{ name = "Streamlit";     url = "http://localhost:8501";             expected = "200" },
    @{ name = "Spark Master";  url = "http://localhost:8080";             expected = "200" },
    @{ name = "MinIO Console"; url = "http://localhost:9001";             expected = "200" }
)

$tcp_services = @(
    @{ name = "Kafka External"; host = "localhost"; port = 9094 },
    @{ name = "Redis";          host = "localhost"; port = 6379 },
    @{ name = "Spark RPC";      host = "localhost"; port = 7077 }
)

$GREEN  = "`e[92m"
$RED    = "`e[91m"
$YELLOW = "`e[93m"
$RESET  = "`e[0m"
$BOLD   = "`e[1m"

Write-Host "`n${BOLD}==========================================================${RESET}"
Write-Host "${BOLD}  DATA LAKEHOUSE — SERVICE HEALTH CHECK${RESET}"
Write-Host "${BOLD}==========================================================${RESET}`n"

# --- HTTP Checks ---
Write-Host "${BOLD}[HTTP Endpoints]${RESET}"
$pass_count = 0
$fail_count = 0

foreach ($svc in $services) {
    try {
        $resp = Invoke-WebRequest -Uri $svc.url -TimeoutSec 5 -UseBasicParsing -ErrorAction Stop
        $status = $resp.StatusCode
        Write-Host "  ${GREEN}[PASS]${RESET} $($svc.name.PadRight(18)) — HTTP $status  $($svc.url)"
        $pass_count++
    }
    catch {
        $err = $_.Exception.Message -replace "`n", " "
        Write-Host "  ${RED}[FAIL]${RESET} $($svc.name.PadRight(18)) — $err"
        $fail_count++
    }
}

# --- TCP Checks ---
Write-Host "`n${BOLD}[TCP Ports]${RESET}"
foreach ($svc in $tcp_services) {
    try {
        $tcp = Test-NetConnection -ComputerName $svc.host -Port $svc.port -WarningAction SilentlyContinue
        if ($tcp.TcpTestSucceeded) {
            Write-Host "  ${GREEN}[PASS]${RESET} $($svc.name.PadRight(18)) — $($svc.host):$($svc.port) OPEN"
            $pass_count++
        } else {
            Write-Host "  ${YELLOW}[WARN]${RESET} $($svc.name.PadRight(18)) — $($svc.host):$($svc.port) CLOSED (Docker chua chay?)"
            $fail_count++
        }
    }
    catch {
        Write-Host "  ${YELLOW}[WARN]${RESET} $($svc.name.PadRight(18)) — Khong kiem tra duoc: $_"
        $fail_count++
    }
}

# --- Docker container status ---
Write-Host "`n${BOLD}[Docker Container Status]${RESET}"
try {
    $containers = docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>&1
    Write-Host $containers
}
catch {
    Write-Host "  ${YELLOW}[WARN]${RESET} Khong the chay docker compose ps: $_"
}

# --- Summary ---
$overall_color = if ($fail_count -eq 0) { $GREEN } else { $RED }
Write-Host "`n${BOLD}==========================================================${RESET}"
Write-Host "  ${overall_color}${BOLD}PASS: $pass_count  |  FAIL/WARN: $fail_count${RESET}"
if ($fail_count -gt 0) {
    Write-Host "  ${YELLOW}Cac service FAIL can chay: docker compose up -d${RESET}"
}
Write-Host "${BOLD}==========================================================${RESET}`n"
