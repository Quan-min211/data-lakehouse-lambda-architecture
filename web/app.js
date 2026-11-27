/**
 * app.js
 * ======
 * Điều khiển giao diện Dashboard Lambda Lakehouse (Theme Sáng / Mô phỏng Binance & Streamlit).
 * Hỗ trợ:
 *  - Kết nối trực tiếp vào Serving API ở nhà (qua localhost hoặc Cloudflare Tunnel)
 *  - Tự động chuyển sang Chế độ Ngủ đông (Demo dữ liệu mẫu + Số liệu thực nghiệm chính thức) khi máy chủ tắt
 *  - Hoàn toàn không hiển thị mã nguồn / câu lệnh kỹ thuật, tập trung thuần túy vào hiển thị và truy vấn dữ liệu.
 */

const COIN_BASE_PRICES = {
    BTCUSDT: 82450.0,
    ETHUSDT: 3120.0,
    SOLUSDT: 188.5,
    BNBUSDT: 635.0,
    XRPUSDT: 1.485,
    DOGEUSDT: 0.365,
    ADAUSDT: 0.985,
    AVAXUSDT: 41.2,
    LINKUSDT: 22.8,
    SUIUSDT: 3.42
};

const STATE = {
    symbol: "BTCUSDT",
    minutes: 60,
    queryWindowMode: "hybrid", // 'hybrid' | 'history' | 'realtime'
    apiBaseUrl: localStorage.getItem("lh_serving_api_url") || "http://localhost:8000",
    autoRefresh: false,
    refreshSeconds: 5,
    refreshTimer: null,
    activeTab: "recon",
    activeBenchSubtab: "overview",
    isLive: false,
    showVwap: true,
    showSpikes: true,
    showWatermark: true,

    // Data payloads
    marketPayload: null,
    reconItems: [],
    quarantineItems: [],
    systemStatus: null,
    latestSpeed: null,
    alerts: []
};

/* ==================== FORMATTING HELPERS ==================== */

function fmtPrice(val) {
    if (val === null || val === undefined || Number.isNaN(Number(val))) return "--";
    const n = Number(val);
    const abs = Math.abs(n);
    let decimals = 2;
    if (abs > 0 && abs < 2) decimals = 4;
    else if (abs >= 2 && abs < 100) decimals = 3;
    return n.toLocaleString("en-US", {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });
}

function fmtNum(val, decimals = 2) {
    if (val === null || val === undefined || Number.isNaN(Number(val))) return "--";
    return Number(val).toLocaleString("en-US", {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });
}

function fmtTimeShort(isoStr) {
    if (!isoStr) return "--:--:--";
    const clean = String(isoStr).replace("T", " ").replace("Z", "");
    return clean.length >= 19 ? clean.slice(11, 19) : clean;
}

function fmtDateTimeUTC(isoStr) {
    if (!isoStr) return "--";
    const clean = String(isoStr).replace("T", " ").replace("Z", "");
    return clean.slice(0, 19);
}

function formatLagVi(seconds) {
    if (seconds === null || seconds === undefined || Number.isNaN(Number(seconds))) return "--";
    const s = Math.max(0, Number(seconds));
    if (s >= 86400) return `${(s / 86400).toFixed(1)} ngày`;
    if (s >= 3600) return `${(s / 3600).toFixed(1)} giờ`;
    if (s >= 60) return `${Math.round(s / 60)} phút`;
    return `${Math.round(s)} giây`;
}

function toggleSidebar(open) {
    const sb = document.getElementById("sidebar");
    const bd = document.getElementById("sidebar-backdrop");
    if (!sb || !bd) return;
    if (open) {
        sb.classList.add("open");
        bd.classList.remove("hidden");
    } else {
        sb.classList.remove("open");
        bd.classList.add("hidden");
    }
}

/* ==================== DETERMINISTIC DEMO DATA GENERATOR (HIBERNATION FALLBACK) ==================== */

function seededRandom(seed) {
    const x = Math.sin(seed) * 10000;
    return x - Math.floor(x);
}

function buildDemoDataset(symbol, minutes, windowMode) {
    const now = new Date();
    now.setUTCSeconds(0, 0);

    // Watermark W sits 12 minutes behind now
    const watermarkDate = new Date(now.getTime() - 12 * 60 * 1000);
    let endTime = new Date(now.getTime());
    let startTime = new Date(endTime.getTime() - minutes * 60 * 1000);

    let queryCase = "case3_hybrid";
    if (windowMode === "history") {
        endTime = new Date(watermarkDate.getTime());
        startTime = new Date(endTime.getTime() - minutes * 60 * 1000);
        queryCase = "case1_history";
    } else if (windowMode === "realtime") {
        startTime = new Date(watermarkDate.getTime());
        endTime = new Date(now.getTime());
        queryCase = "case2_realtime";
    }

    const totalCandles = Math.max(5, Math.round((endTime - startTime) / 60000));
    const basePrice = COIN_BASE_PRICES[symbol] || 100.0;
    const symSeed = symbol.split("").reduce((acc, c) => acc + c.charCodeAt(0), 0);

    const data = [];
    const recon = [];
    let prevClose = basePrice * (1 - 0.004);
    let batchCount = 0;
    let speedCount = 0;

    for (let i = 0; i < totalCandles; i++) {
        const wStart = new Date(startTime.getTime() + i * 60000);
        const wEnd = new Date(wStart.getTime() + 60000);
        const isFinal = wEnd <= watermarkDate;

        const r1 = seededRandom(symSeed + Math.floor(wStart.getTime() / 60000));
        const r2 = seededRandom(symSeed * 3 + i + 17);
        const isSpike = i === Math.floor(totalCandles * 0.38) || i === Math.floor(totalCandles * 0.78);

        const pctMove = isSpike
            ? (r1 > 0.45 ? 0.0235 : -0.022)
            : (r1 - 0.492) * 0.0042;

        const open = prevClose;
        const close = Math.max(basePrice * 0.5, open * (1 + pctMove));
        const wickUp = Math.abs(close - open) * (0.35 + r2 * 0.9) + basePrice * 0.0005;
        const wickDown = Math.abs(close - open) * (0.35 + (1 - r2) * 0.9) + basePrice * 0.0005;
        const high = Math.max(open, close) + wickUp;
        const low = Math.max(0.0001, Math.min(open, close) - wickDown);
        const vwap = (open + high + low + close * 2) / 5;
        const volume = (basePrice > 1000 ? 18 : 950) * (0.6 + r2 * 1.4) * (isSpike ? 3.2 : 1.0);
        const tradeCount = Math.round(280 + r1 * 640 + (isSpike ? 1450 : 0));

        if (isFinal) batchCount++;
        else speedCount++;

        data.push({
            window_start: wStart.toISOString(),
            window_end: wEnd.toISOString(),
            symbol,
            open_price: Number(open.toFixed(4)),
            high_price: Number(high.toFixed(4)),
            low_price: Number(low.toFixed(4)),
            close_price: Number(close.toFixed(4)),
            vwap: Number(vwap.toFixed(4)),
            volume: Number(volume.toFixed(4)),
            trade_count: tradeCount,
            is_spike: isSpike,
            is_final: isFinal,
            source: isFinal ? "batch" : "speed"
        });

        // Build reconciliation entry
        if (isFinal) {
            const healed = i % 5 === 0;
            const driftSign = r1 > 0.5 ? 1 : -1;
            const speedVwap = healed ? vwap + driftSign * basePrice * 0.00042 : vwap;
            const speedVol = healed ? volume * (1 - 0.014) : volume;
            const vwapDelta = Math.abs(vwap - speedVwap);
            const volDriftPct = volume > 0 ? (Math.abs(volume - speedVol) / volume) * 100 : 0;

            recon.push({
                window_start: wStart.toISOString(),
                window_end: wEnd.toISOString(),
                batch_vwap: Number(vwap.toFixed(4)),
                speed_vwap: Number(speedVwap.toFixed(4)),
                vwap_delta: Number(vwapDelta.toFixed(4)),
                batch_volume: Number(volume.toFixed(4)),
                speed_volume: Number(speedVol.toFixed(4)),
                volume_drift_pct: Number(volDriftPct.toFixed(4)),
                reconciliation_status: healed ? "RECONCILED_AND_HEALED" : "MATCHED",
                is_reconciled: true
            });
        } else {
            recon.push({
                window_start: wStart.toISOString(),
                window_end: wEnd.toISOString(),
                batch_vwap: null,
                speed_vwap: Number(vwap.toFixed(4)),
                vwap_delta: null,
                batch_volume: null,
                speed_volume: Number(volume.toFixed(4)),
                volume_drift_pct: null,
                reconciliation_status: "PROVISIONAL_ONLY",
                is_reconciled: false
            });
        }

        prevClose = close;
    }

    let reconStatus = "PARTIALLY_RECONCILED";
    if (queryCase === "case1_history") reconStatus = "RECONCILED";
    else if (queryCase === "case2_realtime") reconStatus = "PROVISIONAL";

    const marketPayload = {
        symbol,
        metadata: {
            query_case: queryCase,
            watermark_used: watermarkDate.toISOString(),
            reconciliation_status: reconStatus,
            batch_candles_count: batchCount,
            speed_candles_count: speedCount
        },
        data
    };

    const quarantineDemo = [
        {
            quarantined_at: new Date(now.getTime() - 8 * 60000).toISOString(),
            symbol,
            trade_id: 984512031,
            price: -12.5,
            quantity: 0.42,
            reason: "INVALID_PRICE"
        },
        {
            quarantined_at: new Date(now.getTime() - 19 * 60000).toISOString(),
            symbol: "ETHUSDT",
            trade_id: 984509814,
            price: 3118.4,
            quantity: 0.0,
            reason: "INVALID_QUANTITY"
        },
        {
            quarantined_at: new Date(now.getTime() - 34 * 60000).toISOString(),
            symbol: "SOLUSDT",
            trade_id: -1,
            price: 188.2,
            quantity: 14.5,
            reason: "INVALID_TRADE_ID"
        },
        {
            quarantined_at: new Date(now.getTime() - 47 * 60000).toISOString(),
            symbol: "BTCUSDT",
            trade_id: 984499102,
            price: 0.0,
            quantity: 0.18,
            reason: "INVALID_PRICE"
        }
    ];

    const lastCandle = data[data.length - 1];
    const alertsDemo = data
        .filter((d) => d.is_spike)
        .slice(-3)
        .reverse()
        .map((d) => ({
            timestamp: d.window_start,
            severity: "WARNING",
            alert_type: "PRICE_SPIKE",
            symbol: d.symbol,
            message: `Biên độ giá biến động mạnh trong cửa sổ 1 phút (Đóng cửa: ${fmtPrice(d.close_price)} USDT)`
        }));

    return {
        marketPayload,
        reconItems: recon.slice().reverse(),
        quarantineItems: quarantineDemo,
        systemStatus: {
            status: "hibernating",
            watermark: {
                watermark_time: watermarkDate.toISOString(),
                freshness_lag_seconds: 720
            },
            dependencies: [
                { name: "clickhouse", status: "hibernating" },
                { name: "redis", status: "hibernating" },
                { name: "kafka", status: "hibernating" },
                { name: "tidb", status: "scale-to-zero" }
            ]
        },
        latestSpeed: {
            cache_status: "demo",
            candle: lastCandle
        },
        alerts: alertsDemo
    };
}

/* ==================== API FETCHING & HYBRID HIBERNATION DETECTION ==================== */

async function fetchJsonWithTimeout(url, timeoutMs = 3200) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
        const res = await fetch(url, { signal: controller.signal });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return await res.json();
    } finally {
        clearTimeout(timer);
    }
}

async function refreshDashboardData() {
    const base = STATE.apiBaseUrl.replace(/\/+$/, "");
    let liveOk = false;

    try {
        const health = await fetchJsonWithTimeout(`${base}/health`, 2200);
        if (health) liveOk = true;
    } catch (_) {
        liveOk = false;
    }

    if (liveOk) {
        try {
            // Determine start_time and end_time based on queryWindowMode
            const statusRes = await fetchJsonWithTimeout(`${base}/api/system/status`, 3500).catch(() => null);
            const now = new Date();
            let endIso = now.toISOString();
            let startIso = new Date(now.getTime() - STATE.minutes * 60000).toISOString();

            const wmIso = statusRes?.watermark?.watermark_time;
            if (wmIso) {
                const wmDate = new Date(wmIso);
                if (!Number.isNaN(wmDate.getTime())) {
                    if (STATE.queryWindowMode === "history") {
                        endIso = wmDate.toISOString();
                        startIso = new Date(wmDate.getTime() - STATE.minutes * 60000).toISOString();
                    } else if (STATE.queryWindowMode === "realtime" && wmDate < now) {
                        startIso = wmDate.toISOString();
                        endIso = now.toISOString();
                    }
                }
            }

            const qs = `symbol=${encodeURIComponent(STATE.symbol)}&start_time=${encodeURIComponent(startIso)}&end_time=${encodeURIComponent(endIso)}`;
            const [market, recon, dq, speed, alerts] = await Promise.all([
                fetchJsonWithTimeout(`${base}/api/market?${qs}`, 5000),
                fetchJsonWithTimeout(`${base}/api/reconciliation?${qs}`, 5000).catch(() => ({ report: [] })),
                fetchJsonWithTimeout(`${base}/api/dq/quarantine?limit=200`, 4000).catch(() => ({ items: [] })),
                fetchJsonWithTimeout(`${base}/api/speed/latest?symbol=${encodeURIComponent(STATE.symbol)}`, 3500).catch(() => null),
                fetchJsonWithTimeout(`${base}/api/system/alerts?limit=30`, 3500).catch(() => ({ alerts: [] }))
            ]);

            if (market && Array.isArray(market.data) && market.data.length > 0) {
                STATE.isLive = true;
                STATE.marketPayload = market;
                STATE.reconItems = Array.isArray(recon?.report) ? recon.report : (Array.isArray(recon?.items) ? recon.items : []);
                STATE.quarantineItems = Array.isArray(dq?.items) ? dq.items : [];
                STATE.systemStatus = statusRes;
                STATE.latestSpeed = speed;
                STATE.alerts = Array.isArray(alerts?.alerts) ? alerts.alerts : (Array.isArray(alerts) ? alerts : []);
                renderAll();
                return;
            }
        } catch (_) {
            // Fall through to demo mode gracefully
        }
    }

    // Hibernating / Demo Mode
    STATE.isLive = false;
    const demo = buildDemoDataset(STATE.symbol, STATE.minutes, STATE.queryWindowMode);
    STATE.marketPayload = demo.marketPayload;
    STATE.reconItems = demo.reconItems;
    STATE.quarantineItems = demo.quarantineItems;
    STATE.systemStatus = demo.systemStatus;
    STATE.latestSpeed = demo.latestSpeed;
    STATE.alerts = demo.alerts;
    renderAll();
}

/* ==================== UI RENDERING ==================== */

function renderAll() {
    renderTopbarAndSidebarStatus();
    renderTickerHeader();
    renderWatermarkBar();
    renderMainCandlestickChart();
    renderReconciliationTab();
    renderBenchmarkTab();
    renderDataQualityTab();
    renderSystemTab();
    renderSpikeTab();

    const nowUtc = new Date().toISOString().replace("T", " ").slice(0, 19);
    const footerEl = document.getElementById("footer-updated-at");
    if (footerEl) footerEl.textContent = `Cập nhật: ${nowUtc} UTC`;
}

function renderTopbarAndSidebarStatus() {
    const srcDot = document.getElementById("chip-source-dot");
    const srcText = document.getElementById("chip-source-text");
    const healthDot = document.getElementById("chip-health-dot");
    const healthText = document.getElementById("chip-health-text");
    const sbSummary = document.getElementById("sidebar-mode-summary");

    if (STATE.isLive) {
        srcDot.style.background = "var(--up-green)";
        srcText.textContent = "Trực tuyến · Máy chủ Serving API";
        const st = String(STATE.systemStatus?.status || "ok").toLowerCase();
        if (st === "ok" || st === "healthy") {
            healthDot.style.background = "var(--up-green)";
            healthText.textContent = "Hệ thống ổn định";
        } else {
            healthDot.style.background = "var(--provisional-amber)";
            healthText.textContent = "Hoạt động một phần";
        }
        sbSummary.innerHTML = `<span style="color:#057A4B;font-weight:600;">● Đang kết nối trực tiếp</span> vào máy chủ dữ liệu của bạn.`;
    } else {
        srcDot.style.background = "var(--provisional-amber)";
        srcText.textContent = "Chế độ Ngủ đông · Dữ liệu minh họa";
        healthDot.style.background = "#94A3B8";
        healthText.textContent = "Máy chủ đang nghỉ";
        sbSummary.innerHTML = `<span style="color:#B45309;font-weight:600;">● Máy chủ tại nhà đang tắt.</span> Giao diện tự động hiển thị dữ liệu mẫu & báo cáo thực nghiệm tĩnh.`;
    }
}

function renderTickerHeader() {
    const rows = STATE.marketPayload?.data || [];
    const sym = STATE.symbol;
    const baseCoin = sym.replace("USDT", "");

    document.getElementById("ticker-symbol-label").innerHTML =
        `<strong class="text-slate-900 text-[15px]">${baseCoin}</strong> / USDT · 1m`;

    if (!rows.length) return;

    const first = rows[0];
    const last = rows[rows.length - 1];
    const openFirst = Number(first.open_price || last.open_price || 0);
    const closeLast = Number(last.close_price || 0);
    const delta = closeLast - openFirst;
    const deltaPct = openFirst > 0 ? (delta / openFirst) * 100 : 0;
    const isUp = delta >= 0;

    const highMax = Math.max(...rows.map((r) => Number(r.high_price || 0)));
    const lowMin = Math.min(...rows.map((r) => Number(r.low_price || 0)));
    const totalVol = rows.reduce((acc, r) => acc + Number(r.volume || 0), 0);
    const totalTrades = rows.reduce((acc, r) => acc + Number(r.trade_count || 0), 0);
    const weightedNotional = rows.reduce((acc, r) => acc + Number(r.vwap || r.close_price || 0) * Number(r.volume || 0), 0);
    const overallVwap = totalVol > 0 ? weightedNotional / totalVol : Number(last.vwap || closeLast);

    const priceEl = document.getElementById("ticker-price");
    priceEl.textContent = fmtPrice(closeLast);
    priceEl.style.color = isUp ? "var(--up-green)" : "var(--down-red)";

    const pillEl = document.getElementById("ticker-delta-pill");
    const sign = isUp ? "+" : "";
    pillEl.textContent = `${sign}${fmtPrice(delta)} (${sign}${deltaPct.toFixed(2)}%)`;
    pillEl.className = `lh-badge lh-num ${isUp ? "badge-green" : "badge-red"}`;

    const badgeEl = document.getElementById("ticker-candle-badge");
    if (last.is_final || last.source === "batch") {
        badgeEl.textContent = "Đã chốt sổ";
        badgeEl.className = "lh-badge badge-green";
    } else {
        badgeEl.textContent = "Tạm tính";
        badgeEl.className = "lh-badge badge-amber";
    }

    document.getElementById("stat-open").textContent = fmtPrice(openFirst);
    document.getElementById("stat-high").textContent = fmtPrice(highMax);
    document.getElementById("stat-low").textContent = fmtPrice(lowMin);
    document.getElementById("stat-vwap").textContent = fmtPrice(overallVwap);
    document.getElementById("stat-volume").textContent = fmtNum(totalVol, 2);
    document.getElementById("stat-trades").textContent = Math.round(totalTrades).toLocaleString("en-US");
}

function renderWatermarkBar() {
    const meta = STATE.marketPayload?.metadata || {};
    const qCase = String(meta.query_case || "case3_hybrid");
    const reconStatus = String(meta.reconciliation_status || "PARTIALLY_RECONCILED");
    const batchN = Number(meta.batch_candles_count || 0);
    const speedN = Number(meta.speed_candles_count || 0);
    const total = Math.max(1, batchN + speedN);

    const routeLabels = {
        case1_history: "Trường hợp 1 · Lịch sử chuẩn tắc",
        case2_realtime: "Trường hợp 2 · Thời gian thực",
        case3_hybrid: "Trường hợp 3 · Kết hợp hai luồng"
    };
    document.getElementById("wm-route-title").textContent = routeLabels[qCase] || qCase;

    const statusBadge = document.getElementById("wm-status-badge");
    if (reconStatus === "RECONCILED") {
        statusBadge.textContent = "Đã chốt sổ 100%";
        statusBadge.className = "lh-badge badge-green";
    } else if (reconStatus === "PROVISIONAL") {
        statusBadge.textContent = "Đang tạm tính";
        statusBadge.className = "lh-badge badge-amber";
    } else {
        statusBadge.textContent = "Đối soát một phần";
        statusBadge.className = "lh-badge badge-blue";
    }

    const batchPct = Math.round((batchN / total) * 100);
    const speedPct = 100 - batchPct;
    document.getElementById("wm-bar-batch").style.width = `${batchPct}%`;
    document.getElementById("wm-bar-speed").style.width = `${speedPct}%`;
    document.getElementById("wm-candle-split").textContent = `${batchN} nến chuẩn · ${speedN} nến tạm tính`;

    const wmStr = meta.watermark_used || STATE.systemStatus?.watermark?.watermark_time;
    if (wmStr) {
        document.getElementById("wm-time-clock").textContent = fmtTimeShort(wmStr);
        document.getElementById("wm-time-date").textContent = `${String(wmStr).slice(0, 10)} UTC`;
        const wmMs = new Date(wmStr).getTime();
        const lagSec = Math.max(0, (Date.now() - wmMs) / 1000);
        document.getElementById("wm-lag-value").textContent = formatLagVi(lagSec);
    } else {
        document.getElementById("wm-time-clock").textContent = "--:--:--";
        document.getElementById("wm-time-date").textContent = "Chưa ghi nhận mốc W";
        document.getElementById("wm-lag-value").textContent = "--";
    }

    // Average reconciliation drift
    const driftEl = document.getElementById("wm-drift-value");
    const driftNote = document.getElementById("wm-drift-note");
    if (qCase === "case1_history") {
        driftEl.textContent = "0.0000";
        driftEl.style.color = "var(--up-green)";
        driftNote.textContent = "Dữ liệu lịch sử chuẩn tắc tuyệt đối";
    } else {
        const deltas = STATE.reconItems
            .map((r) => r.vwap_delta)
            .filter((v) => v !== null && v !== undefined && !Number.isNaN(Number(v)))
            .map(Number);
        if (deltas.length > 0) {
            const avgDelta = deltas.reduce((a, b) => a + b, 0) / deltas.length;
            driftEl.textContent = `${fmtNum(avgDelta, 4)} USDT`;
            driftEl.style.color = avgDelta < 0.001 ? "var(--up-green)" : "var(--text-primary)";
            driftNote.textContent = `Tính trên ${deltas.length} cửa sổ đã đối chiếu`;
        } else {
            driftEl.textContent = "Đang chờ chốt sổ";
            driftEl.style.color = "var(--provisional-amber)";
            driftNote.textContent = "Chưa có cửa sổ đối chiếu trong khung này";
        }
    }
}

/* ==================== MAIN PLOTLY CANDLESTICK CHART (WHITE THEME) ==================== */

function renderMainCandlestickChart() {
    const rows = STATE.marketPayload?.data || [];
    const el = document.getElementById("main-candlestick-chart");
    if (!el || !rows.length) return;

    const times = rows.map((r) => r.window_start);
    const opens = rows.map((r) => Number(r.open_price));
    const highs = rows.map((r) => Number(r.high_price));
    const lows = rows.map((r) => Number(r.low_price));
    const closes = rows.map((r) => Number(r.close_price));
    const vwaps = rows.map((r) => Number(r.vwap));
    const vols = rows.map((r) => Number(r.volume));
    const volColors = rows.map((r) =>
        Number(r.close_price) >= Number(r.open_price)
            ? "rgba(14, 203, 129, 0.55)"
            : "rgba(246, 70, 93, 0.55)"
    );

    const traces = [
        {
            type: "candlestick",
            name: "Nến 1 phút",
            x: times,
            open: opens,
            high: highs,
            low: lows,
            close: closes,
            increasing: { line: { color: "#0ECB81", width: 1.3 }, fillcolor: "#0ECB81" },
            decreasing: { line: { color: "#F6465D", width: 1.3 }, fillcolor: "#F6465D" },
            xaxis: "x",
            yaxis: "y"
        },
        {
            type: "bar",
            name: "Khối lượng",
            x: times,
            y: vols,
            marker: { color: volColors },
            xaxis: "x",
            yaxis: "y2",
            hovertemplate: "Khối lượng: %{y:,.2f}<extra></extra>"
        }
    ];

    if (STATE.showVwap) {
        traces.push({
            type: "scatter",
            mode: "lines",
            name: "VWAP",
            x: times,
            y: vwaps,
            line: { color: "#2563EB", width: 1.8 },
            xaxis: "x",
            yaxis: "y",
            hovertemplate: "VWAP: %{y:,.4f}<extra></extra>"
        });
    }

    if (STATE.showSpikes) {
        const spikeRows = rows.filter((r) => r.is_spike);
        if (spikeRows.length > 0) {
            traces.push({
                type: "scatter",
                mode: "markers",
                name: "Biến động mạnh",
                x: spikeRows.map((r) => r.window_start),
                y: spikeRows.map((r) => Number(r.high_price) * 1.0012),
                marker: {
                    symbol: "diamond",
                    size: 9,
                    color: "#F59E0B",
                    line: { color: "#B45309", width: 1 }
                },
                xaxis: "x",
                yaxis: "y",
                hovertemplate: "Điểm biến động mạnh<br>Đỉnh: %{y:,.2f}<extra></extra>"
            });
        }
    }

    const shapes = [];
    const annotations = [];
    const wmIso = STATE.marketPayload?.metadata?.watermark_used;
    if (STATE.showWatermark && wmIso && times.length > 1) {
        const wmTime = new Date(wmIso).getTime();
        const tStart = new Date(times[0]).getTime();
        const tEnd = new Date(times[times.length - 1]).getTime();

        if (wmTime >= tStart && wmTime <= tEnd) {
            shapes.push(
                {
                    type: "rect",
                    xref: "x",
                    yref: "paper",
                    x0: wmIso,
                    x1: times[times.length - 1],
                    y0: 0.26,
                    y1: 1.0,
                    fillcolor: "rgba(245, 158, 11, 0.06)",
                    line: { width: 0 }
                },
                {
                    type: "line",
                    xref: "x",
                    yref: "paper",
                    x0: wmIso,
                    x1: wmIso,
                    y0: 0,
                    y1: 1,
                    line: { color: "#D97706", width: 1.5, dash: "dash" }
                }
            );
            annotations.push({
                xref: "x",
                yref: "paper",
                x: wmIso,
                y: 0.99,
                text: "Mốc chốt sổ W",
                showarrow: false,
                xanchor: "left",
                xshift: 6,
                font: { size: 11, color: "#B45309", family: "Inter, sans-serif" },
                bgcolor: "rgba(254, 243, 199, 0.9)",
                borderpad: 3
            });
        }
    }

    const layout = {
        paper_bgcolor: "#FFFFFF",
        plot_bgcolor: "#FFFFFF",
        margin: { l: 56, r: 24, t: 10, b: 34 },
        font: { family: "Inter, sans-serif", size: 11.5, color: "#474D57" },
        showlegend: false,
        hovermode: "x unified",
        xaxis: {
            type: "date",
            rangeslider: { visible: false },
            gridcolor: "#F1F5F9",
            linecolor: "#E2E8F0",
            tickfont: { color: "#707A8A" }
        },
        yaxis: {
            domain: [0.27, 1.0],
            gridcolor: "#F1F5F9",
            linecolor: "#E2E8F0",
            side: "right",
            tickfont: { color: "#474D57" }
        },
        yaxis2: {
            domain: [0.0, 0.21],
            gridcolor: "#F1F5F9",
            linecolor: "#E2E8F0",
            side: "right",
            tickfont: { color: "#707A8A" }
        },
        shapes,
        annotations
    };

    Plotly.react(el, traces, layout, { responsive: true, displayModeBar: false });
}

/* ==================== TAB 1: RECONCILIATION ==================== */

const RECON_STATUS_VI = {
    MATCHED: { text: "Khớp tuyệt đối", cls: "badge-green" },
    RECONCILED_AND_HEALED: { text: "Đã hiệu chỉnh sai lệch", cls: "badge-blue" },
    PROVISIONAL_ONLY: { text: "Đang tạm tính", cls: "badge-amber" },
    BATCH_ONLY: { text: "Chỉ có dữ liệu chuẩn", cls: "badge-slate" }
};

function renderReconciliationTab() {
    const items = STATE.reconItems || [];
    const filterStatus = document.getElementById("recon-status-filter")?.value || "ALL";
    const searchQ = (document.getElementById("recon-search-input")?.value || "").trim().toLowerCase();

    const total = items.length;
    const reconciledItems = items.filter((r) => r.is_reconciled || r.reconciliation_status === "MATCHED" || r.reconciliation_status === "RECONCILED_AND_HEALED");
    const reconciledCount = reconciledItems.length;
    const coverage = total > 0 ? ((reconciledCount / total) * 100).toFixed(1) : "0.0";

    const validDeltas = items
        .map((r) => r.vwap_delta)
        .filter((v) => v !== null && v !== undefined && !Number.isNaN(Number(v)))
        .map((v) => Math.abs(Number(v)));

    const meanDelta = validDeltas.length ? fmtNum(validDeltas.reduce((a, b) => a + b, 0) / validDeltas.length, 4) : "--";
    const maxDelta = validDeltas.length ? fmtNum(Math.max(...validDeltas), 4) : "--";

    document.getElementById("recon-kpi-total").textContent = String(total);
    document.getElementById("recon-kpi-reconciled").textContent = String(reconciledCount);
    document.getElementById("recon-kpi-coverage").textContent = `Đạt ${coverage}% số cửa sổ trong khung`;
    document.getElementById("recon-kpi-mean").textContent = meanDelta;
    document.getElementById("recon-kpi-max").textContent = maxDelta;

    const filtered = items.filter((r) => {
        if (filterStatus !== "ALL" && r.reconciliation_status !== filterStatus) return false;
        if (searchQ && !String(r.window_start || "").toLowerCase().includes(searchQ)) return false;
        return true;
    });

    const tbody = document.getElementById("recon-table-body");
    if (!filtered.length) {
        tbody.innerHTML = `<tr><td colspan="9" class="text-center text-slate-400 py-6">Không có cửa sổ đối soát nào khớp với bộ lọc hiện tại.</td></tr>`;
    } else {
        tbody.innerHTML = filtered
            .slice(0, 120)
            .map((r) => {
                const st = RECON_STATUS_VI[r.reconciliation_status] || { text: r.reconciliation_status || "--", cls: "badge-slate" };
                return `<tr>
                    <td>${fmtTimeShort(r.window_start)}</td>
                    <td>${fmtTimeShort(r.window_end)}</td>
                    <td><span class="lh-badge ${st.cls}">${st.text}</span></td>
                    <td class="text-right">${r.batch_vwap != null ? fmtPrice(r.batch_vwap) : "--"}</td>
                    <td class="text-right">${r.speed_vwap != null ? fmtPrice(r.speed_vwap) : "--"}</td>
                    <td class="text-right">${r.vwap_delta != null ? fmtNum(r.vwap_delta, 4) : "--"}</td>
                    <td class="text-right">${r.batch_volume != null ? fmtNum(r.batch_volume, 3) : "--"}</td>
                    <td class="text-right">${r.speed_volume != null ? fmtNum(r.speed_volume, 3) : "--"}</td>
                    <td class="text-right">${r.volume_drift_pct != null ? fmtNum(r.volume_drift_pct, 3) + "%" : "--"}</td>
                </tr>`;
            })
            .join("");
    }

    // Render VWAP compare chart if tab is visible
    if (STATE.activeTab === "recon") {
        const chronological = items.slice().reverse();
        const chartEl = document.getElementById("recon-compare-chart");
        if (chartEl && chronological.length) {
            Plotly.react(
                chartEl,
                [
                    {
                        type: "scatter",
                        mode: "lines",
                        name: "VWAP Chuẩn (Batch)",
                        x: chronological.map((r) => r.window_start),
                        y: chronological.map((r) => r.batch_vwap),
                        line: { color: "#0ECB81", width: 2.2 }
                    },
                    {
                        type: "scatter",
                        mode: "lines",
                        name: "VWAP Tạm tính (Speed)",
                        x: chronological.map((r) => r.window_start),
                        y: chronological.map((r) => r.speed_vwap),
                        line: { color: "#F59E0B", width: 1.6, dash: "dot" }
                    }
                ],
                {
                    paper_bgcolor: "#FFFFFF",
                    plot_bgcolor: "#FFFFFF",
                    margin: { l: 48, r: 20, t: 10, b: 32 },
                    font: { family: "Inter, sans-serif", size: 11.5, color: "#474D57" },
                    legend: { orientation: "h", y: 1.14, x: 0 },
                    xaxis: { gridcolor: "#F1F5F9", linecolor: "#E2E8F0" },
                    yaxis: { gridcolor: "#F1F5F9", linecolor: "#E2E8F0", side: "right" }
                },
                { responsive: true, displayModeBar: false }
            );
        }
    }
}

/* ==================== TAB 2: BENCHMARK ==================== */

function renderBenchmarkTab() {
    const bm = window.OFFICIAL_BENCHMARKS;
    if (!bm) return;

    // Overview Chaos Table
    const chaosBody = document.getElementById("chaos-table-body");
    if (chaosBody) {
        chaosBody.innerHTML = bm.chaos_tests
            .map(
                (c) => `<tr>
                    <td class="font-semibold text-slate-800">${c.code}</td>
                    <td>
                        <div class="font-medium text-slate-800">${c.name}</div>
                        <div class="text-[11.5px] text-slate-500">${c.fault}</div>
                    </td>
                    <td class="text-amber-700">${c.pre_drift}</td>
                    <td class="text-emerald-700 font-medium">${c.post_drift}</td>
                    <td class="text-right lh-num">${(c.t_conv_s * 1000).toFixed(1)} ms</td>
                    <td><span class="lh-badge badge-green">${c.status}</span></td>
                </tr>`
            )
            .join("");
    }

    // BM1 Table & Chart
    document.getElementById("bm1-insight").textContent = bm.bm1_latency.insight;
    document.getElementById("bm1-table-body").innerHTML = bm.bm1_latency.scenarios
        .map(
            (s) => `<tr>
                <td class="font-semibold">${s.query}</td>
                <td class="text-slate-500">${s.range}</td>
                <td class="text-right">${s.rows}</td>
                <td class="text-right">${s.batch_only.p50.toFixed(2)}</td>
                <td class="text-right">${s.speed_only.p50.toFixed(2)}</td>
                <td class="text-right font-semibold text-blue-600">${s.lambda.p50.toFixed(2)}</td>
                <td class="text-right">${s.lambda.p95.toFixed(2)}</td>
            </tr>`
        )
        .join("");

    // BM2 Table
    document.getElementById("bm2-insight").textContent = bm.bm2_reprocess.insight;
    document.getElementById("bm2-table-body").innerHTML = bm.bm2_reprocess.runs
        .map(
            (r) => `<tr>
                <td class="font-semibold">${r.symbol}</td>
                <td class="text-slate-500">${r.window}</td>
                <td class="text-right">${r.matched_candles}</td>
                <td class="text-right"><span class="lh-badge badge-green">${r.coverage_pct.toFixed(1)}%</span></td>
                <td class="text-right">${r.mae.toFixed(4)} USDT</td>
                <td class="text-right">${r.mape_pct.toFixed(4)}%</td>
                <td class="text-right font-bold text-emerald-600">0.000%</td>
            </tr>`
        )
        .join("");

    // BM3 Insight
    document.getElementById("bm3-insight").textContent = bm.bm3_compaction.insight;

    if (STATE.activeTab !== "bench") return;

    const basePlotLayout = {
        paper_bgcolor: "#FFFFFF",
        plot_bgcolor: "#FFFFFF",
        margin: { l: 48, r: 20, t: 16, b: 38 },
        font: { family: "Inter, sans-serif", size: 12, color: "#474D57" },
        legend: { orientation: "h", y: 1.14, x: 0 },
        xaxis: { gridcolor: "#F1F5F9", linecolor: "#E2E8F0" },
        yaxis: { gridcolor: "#F1F5F9", linecolor: "#E2E8F0" }
    };

    if (STATE.activeBenchSubtab === "bm1") {
        const sc = bm.bm1_latency.scenarios;
        Plotly.react(
            document.getElementById("bm1-chart"),
            [
                {
                    type: "bar",
                    name: "Chỉ luồng Lô (P50)",
                    x: sc.map((s) => s.query),
                    y: sc.map((s) => s.batch_only.p50),
                    marker: { color: "#0ECB81" }
                },
                {
                    type: "bar",
                    name: "Chỉ luồng Nhanh (P50)",
                    x: sc.map((s) => s.query),
                    y: sc.map((s) => s.speed_only.p50),
                    marker: { color: "#F59E0B" }
                },
                {
                    type: "bar",
                    name: "Kết hợp Lambda (P50)",
                    x: sc.map((s) => s.query),
                    y: sc.map((s) => s.lambda.p50),
                    marker: { color: "#2563EB" }
                }
            ],
            { ...basePlotLayout, barmode: "group", yaxis: { ...basePlotLayout.yaxis, title: "Thời gian phản hồi (ms)" } },
            { responsive: true, displayModeBar: false }
        );
    } else if (STATE.activeBenchSubtab === "bm2") {
        const runs = bm.bm2_reprocess.runs;
        Plotly.react(
            document.getElementById("bm2-chart"),
            [
                {
                    type: "bar",
                    name: "Tỷ lệ lệch tạm thời trước chốt sổ (MAPE %)",
                    x: runs.map((r) => r.symbol),
                    y: runs.map((r) => r.mape_pct),
                    marker: { color: "#F59E0B" }
                },
                {
                    type: "bar",
                    name: "Sai số sau khi tái xử lý & nâng mốc W (%)",
                    x: runs.map((r) => r.symbol),
                    y: runs.map((r) => r.post_batch_error),
                    marker: { color: "#0ECB81" }
                }
            ],
            { ...basePlotLayout, barmode: "group", yaxis: { ...basePlotLayout.yaxis, title: "Tỷ lệ sai lệch (%)" } },
            { responsive: true, displayModeBar: false }
        );
    } else if (STATE.activeBenchSubtab === "bm3") {
        const c = bm.bm3_compaction;
        const xRuns = c.series_before.map((_, idx) => `Lần ${idx + 1}`);
        Plotly.react(
            document.getElementById("bm3-chart"),
            [
                {
                    type: "scatter",
                    mode: "lines+markers",
                    name: "Trước khi gộp tệp (118 tệp nhỏ)",
                    x: xRuns,
                    y: c.series_before,
                    line: { color: "#F6465D", width: 2 }
                },
                {
                    type: "scatter",
                    mode: "lines+markers",
                    name: "Sau khi gộp tệp (10 tệp tối ưu)",
                    x: xRuns,
                    y: c.series_after,
                    line: { color: "#0ECB81", width: 2.2 }
                }
            ],
            { ...basePlotLayout, yaxis: { ...basePlotLayout.yaxis, title: "Thời gian đọc (ms)" } },
            { responsive: true, displayModeBar: false }
        );
    }
}

/* ==================== TAB 3: DATA QUALITY ==================== */

const DQ_REASON_VI = {
    INVALID_PRICE: "Giá không hợp lệ (≤ 0)",
    INVALID_QUANTITY: "Khối lượng không hợp lệ (≤ 0)",
    INVALID_TRADE_ID: "Mã giao dịch không hợp lệ",
    MISSING_SYMBOL: "Thiếu thông tin cặp giao dịch"
};

function renderDataQualityTab() {
    const items = STATE.quarantineItems || [];
    const reasonFilter = document.getElementById("dq-reason-filter")?.value || "ALL";

    const bannerDot = document.getElementById("dq-banner-dot");
    const bannerText = document.getElementById("dq-banner-text");
    const countBadge = document.getElementById("dq-count-badge");

    countBadge.textContent = `${items.length} bản ghi cách ly`;

    if (STATE.isLive && items.length === 0) {
        bannerDot.style.background = "var(--up-green)";
        bannerText.textContent = "100% dữ liệu giao dịch hợp lệ — Không phát hiện bản ghi vi phạm nào trong khu vực cách ly.";
        countBadge.className = "lh-badge badge-green lh-num";
    } else if (STATE.isLive) {
        bannerDot.style.background = "var(--provisional-amber)";
        bannerText.textContent = `Đã tự động chặn và cách ly ${items.length} giao dịch bất thường, bảo vệ an toàn cho dữ liệu tổng hợp.`;
        countBadge.className = "lh-badge badge-amber lh-num";
    } else {
        bannerDot.style.background = "var(--provisional-amber)";
        bannerText.textContent = "Đang hiển thị danh sách mẫu các giao dịch lỗi bị chốt chặn chất lượng tự động tách riêng.";
        countBadge.className = "lh-badge badge-amber lh-num";
    }

    const filtered = items.filter((r) => {
        const code = String(r.reason || r.error_reason || "");
        if (reasonFilter !== "ALL" && !code.includes(reasonFilter)) return false;
        return true;
    });

    const tbody = document.getElementById("dq-table-body");
    if (!filtered.length) {
        tbody.innerHTML = `<tr><td colspan="6" class="text-center text-slate-400 py-6">Không có bản ghi cách ly nào trong mục đã chọn.</td></tr>`;
    } else {
        tbody.innerHTML = filtered
            .map((r) => {
                const rawReason = String(r.reason || r.error_reason || "Vi phạm quy tắc dữ liệu");
                const viReason = DQ_REASON_VI[rawReason] || rawReason;
                return `<tr>
                    <td>${fmtDateTimeUTC(r.quarantined_at || r.ingested_at)}</td>
                    <td class="font-semibold">${r.symbol || "--"}</td>
                    <td>${r.trade_id ?? "--"}</td>
                    <td class="text-right text-rose-600 font-medium">${r.price ?? "--"}</td>
                    <td class="text-right">${r.quantity ?? "--"}</td>
                    <td><span class="lh-badge badge-red">${viReason}</span></td>
                </tr>`;
            })
            .join("");
    }

    if (STATE.activeTab === "dq") {
        const byReason = {};
        const bySymbol = {};
        items.forEach((r) => {
            const rawReason = String(r.reason || r.error_reason || "Khác");
            const label = DQ_REASON_VI[rawReason] || rawReason;
            byReason[label] = (byReason[label] || 0) + 1;
            const sym = String(r.symbol || "Khác");
            bySymbol[sym] = (bySymbol[sym] || 0) + 1;
        });

        Plotly.react(
            document.getElementById("dq-reason-chart"),
            [
                {
                    type: "pie",
                    hole: 0.55,
                    labels: Object.keys(byReason).length ? Object.keys(byReason) : ["Dữ liệu sạch"],
                    values: Object.keys(byReason).length ? Object.values(byReason) : [1],
                    marker: { colors: Object.keys(byReason).length ? ["#F6465D", "#F59E0B", "#2563EB", "#8B5CF6"] : ["#0ECB81"] }
                }
            ],
            {
                paper_bgcolor: "#FFFFFF",
                margin: { l: 16, r: 16, t: 12, b: 16 },
                font: { family: "Inter, sans-serif", size: 11.5, color: "#474D57" },
                showlegend: true
            },
            { responsive: true, displayModeBar: false }
        );

        Plotly.react(
            document.getElementById("dq-symbol-chart"),
            [
                {
                    type: "bar",
                    x: Object.keys(bySymbol),
                    y: Object.values(bySymbol),
                    marker: { color: "#2563EB" }
                }
            ],
            {
                paper_bgcolor: "#FFFFFF",
                plot_bgcolor: "#FFFFFF",
                margin: { l: 40, r: 16, t: 12, b: 32 },
                font: { family: "Inter, sans-serif", size: 11.5, color: "#474D57" },
                xaxis: { gridcolor: "#F1F5F9" },
                yaxis: { gridcolor: "#F1F5F9", dtick: 1 }
            },
            { responsive: true, displayModeBar: false }
        );
    }
}

/* ==================== TAB 4: SYSTEM STATUS ==================== */

const ALERT_LABELS_VI = {
    PRICE_SPIKE: "Đột biến giá",
    VOLUME_DRIFT: "Lệch khối lượng",
    STREAMING_LAG: "Trễ mốc chốt sổ",
    QUARANTINE_SPIKE: "Tăng bản ghi lỗi",
    SEQUENCE_GAP: "Thiếu chuỗi giao dịch"
};

function statusVisual(raw) {
    const s = String(raw || "").toLowerCase();
    if (["ok", "up", "healthy", "connected"].includes(s)) {
        return { text: "ĐANG HOẠT ĐỘNG", color: "var(--up-green)" };
    }
    if (["hibernating", "scale-to-zero"].includes(s)) {
        return { text: "NGỦ ĐÔNG (TIẾT KIỆM)", color: "var(--provisional-amber)" };
    }
    if (["degraded", "warning", "slow"].includes(s)) {
        return { text: "CẢNH BÁO CHẬM", color: "var(--provisional-amber)" };
    }
    return { text: "NGẮT KẾT NỐI", color: "var(--down-red)" };
}

function renderSystemTab() {
    const depList = STATE.systemStatus?.dependencies || [];
    const depMap = {};
    depList.forEach((d) => {
        depMap[String(d.name || "").toLowerCase()] = d.status;
    });

    const services = [
        { name: "Cổng Truy vấn (API)", role: "Hợp nhất kết quả tra cứu", st: STATE.isLive ? (STATE.systemStatus?.status || "ok") : "hibernating" },
        { name: "Kho Phân tích (ClickHouse)", role: "Lưu trữ nến chuẩn & tạm tính", st: depMap.clickhouse || (STATE.isLive ? "ok" : "hibernating") },
        { name: "Bộ nhớ Đệm (Redis)", role: "Phục vụ nến thời gian thực", st: depMap.redis || (STATE.isLive ? "ok" : "hibernating") },
        { name: "Luồng Sự kiện (Kafka)", role: "Tiếp nhận giao dịch trực tiếp", st: depMap.kafka || (STATE.isLive ? "ok" : "hibernating") },
        { name: "Đám mây TiDB Serverless", role: "Chuẩn hóa dữ liệu tự động co giãn", st: STATE.isLive ? "ok" : "scale-to-zero" }
    ];

    document.getElementById("sys-services-grid").innerHTML = services
        .map((svc) => {
            const vis = statusVisual(svc.st);
            return `<div class="lh-cell">
                <div class="lh-cell-label">${svc.name}</div>
                <div class="flex items-center gap-2 my-0.5">
                    <span class="lh-dot" style="background:${vis.color};"></span>
                    <span class="font-bold text-[13px]" style="color:${vis.color};">${vis.text}</span>
                </div>
                <div class="lh-cell-note">${svc.role}</div>
            </div>`;
        })
        .join("");

    // Latest speed candle
    const candle = STATE.latestSpeed?.candle;
    const cacheBadge = document.getElementById("sys-cache-badge");
    if (candle) {
        cacheBadge.textContent = STATE.isLive ? "Đọc trực tiếp từ bộ nhớ đệm" : "Dữ liệu minh họa";
        cacheBadge.className = `lh-badge ${STATE.isLive ? "badge-green" : "badge-amber"}`;
        document.getElementById("sys-latest-symbol").textContent = candle.symbol || STATE.symbol;
        document.getElementById("sys-latest-close").textContent = `${fmtPrice(candle.close_price)} USDT`;
        document.getElementById("sys-latest-vwap").textContent = `${fmtPrice(candle.vwap)} USDT`;
        document.getElementById("sys-latest-trades").textContent = Number(candle.trade_count || 0).toLocaleString("en-US");
    } else {
        cacheBadge.textContent = "Chưa có dữ liệu";
        cacheBadge.className = "lh-badge badge-slate";
    }

    // Alerts
    const alerts = STATE.alerts || [];
    document.getElementById("sys-alert-count").textContent = `${alerts.length} cảnh báo`;
    const alertsBody = document.getElementById("sys-alerts-body");
    if (!alerts.length) {
        alertsBody.innerHTML = `<tr><td colspan="5" class="text-center text-emerald-600 font-medium py-6">Hệ thống vận hành bình thường — Không có cảnh báo bất thường nào.</td></tr>`;
    } else {
        alertsBody.innerHTML = alerts
            .map((a) => {
                const sev = String(a.severity || "INFO").toUpperCase();
                const sevBadge = sev === "CRITICAL" ? "badge-red" : sev === "WARNING" ? "badge-amber" : "badge-blue";
                const sevVi = sev === "CRITICAL" ? "Nghiêm trọng" : sev === "WARNING" ? "Cảnh báo" : "Thông tin";
                const typeVi = ALERT_LABELS_VI[a.alert_type] || a.alert_type || "--";
                return `<tr>
                    <td class="lh-num">${fmtDateTimeUTC(a.timestamp)}</td>
                    <td><span class="lh-badge ${sevBadge}">${sevVi}</span></td>
                    <td class="font-medium">${typeVi}</td>
                    <td class="font-semibold">${a.symbol || STATE.symbol}</td>
                    <td class="whitespace-normal">${a.message || "--"}</td>
                </tr>`;
            })
            .join("");
    }
}

/* ==================== TAB 5: PRICE SPIKES ==================== */

function renderSpikeTab() {
    const rows = (STATE.marketPayload?.data || []).filter((r) => r.is_spike);
    document.getElementById("spike-count-badge").textContent = `${rows.length} điểm biến động`;

    const tbody = document.getElementById("spike-table-body");
    if (!rows.length) {
        tbody.innerHTML = `<tr><td colspan="10" class="text-center text-slate-400 py-6">Không có cây nến biến động mạnh nào trong khung thời gian đang chọn.</td></tr>`;
        return;
    }

    tbody.innerHTML = rows
        .slice()
        .reverse()
        .map((r) => {
            const o = Number(r.open_price || 0);
            const c = Number(r.close_price || 0);
            const h = Number(r.high_price || 0);
            const l = Number(r.low_price || 0);
            const bodyPct = o > 0 ? (Math.abs(c - o) / o) * 100 : 0;
            const rangePct = l > 0 ? ((h - l) / l) * 100 : 0;
            const isUp = c >= o;
            return `<tr>
                <td>${fmtDateTimeUTC(r.window_start)}</td>
                <td class="text-right">${fmtPrice(o)}</td>
                <td class="text-right font-semibold" style="color:${isUp ? "var(--up-green)" : "var(--down-red)"};">${fmtPrice(c)}</td>
                <td class="text-right">${fmtPrice(h)}</td>
                <td class="text-right">${fmtPrice(l)}</td>
                <td class="text-right font-semibold">${bodyPct.toFixed(2)}%</td>
                <td><span class="lh-badge ${isUp ? "badge-green" : "badge-red"}">${isUp ? "▲ Tăng mạnh" : "▼ Giảm mạnh"}</span></td>
                <td class="text-right">${rangePct.toFixed(2)}%</td>
                <td class="text-right">${fmtNum(r.volume, 2)}</td>
                <td><span class="lh-badge ${r.is_final ? "badge-green" : "badge-amber"}">${r.is_final ? "Đã chốt sổ" : "Tạm tính"}</span></td>
            </tr>`;
        })
        .join("");
}

/* ==================== EVENT LISTENERS & INIT ==================== */

function switchTab(tabId) {
    STATE.activeTab = tabId;
    document.querySelectorAll(".lh-tab-btn").forEach((btn) => {
        btn.classList.toggle("active", btn.dataset.tab === tabId);
    });
    document.querySelectorAll(".tab-panel").forEach((panel) => {
        panel.classList.toggle("hidden", panel.id !== `tab-${tabId}`);
    });
    if (tabId === "recon") renderReconciliationTab();
    if (tabId === "bench") renderBenchmarkTab();
    if (tabId === "dq") renderDataQualityTab();
}

function updateAutoRefreshTimer() {
    if (STATE.refreshTimer) {
        clearInterval(STATE.refreshTimer);
        STATE.refreshTimer = null;
    }
    const box = document.getElementById("refresh-interval-box");
    if (STATE.autoRefresh) {
        box.classList.remove("hidden");
        STATE.refreshTimer = setInterval(() => {
            refreshDashboardData();
        }, STATE.refreshSeconds * 1000);
    } else {
        box.classList.add("hidden");
    }
}

function startUtcClock() {
    const tick = () => {
        const now = new Date().toISOString().slice(11, 19);
        const el = document.getElementById("chip-clock");
        if (el) el.textContent = `${now} UTC`;
    };
    tick();
    setInterval(tick, 1000);
}

document.addEventListener("DOMContentLoaded", () => {
    // Restore saved API URL
    const apiInput = document.getElementById("api-url-input");
    apiInput.value = STATE.apiBaseUrl;

    // Symbol select & quick pills
    const symSelect = document.getElementById("symbol-select");
    symSelect.addEventListener("change", () => {
        STATE.symbol = symSelect.value;
        document.querySelectorAll("#quick-coins .lh-coin-pill").forEach((b) => {
            b.classList.toggle("active", b.dataset.coin === STATE.symbol);
        });
        refreshDashboardData();
    });

    document.querySelectorAll("#quick-coins .lh-coin-pill").forEach((btn) => {
        btn.addEventListener("click", () => {
            STATE.symbol = btn.dataset.coin;
            symSelect.value = STATE.symbol;
            document.querySelectorAll("#quick-coins .lh-coin-pill").forEach((b) => {
                b.classList.toggle("active", b.dataset.coin === STATE.symbol);
            });
            refreshDashboardData();
        });
    });

    // Timeframe buttons
    document.querySelectorAll("#timeframe-group .lh-seg-btn").forEach((btn) => {
        btn.addEventListener("click", () => {
            STATE.minutes = Number(btn.dataset.minutes || 60);
            document.querySelectorAll("#timeframe-group .lh-seg-btn").forEach((b) => {
                b.classList.toggle("active", b === btn);
            });
            refreshDashboardData();
        });
    });

    // Query window mode
    document.getElementById("query-window-mode").addEventListener("change", (e) => {
        STATE.queryWindowMode = e.target.value;
        refreshDashboardData();
    });

    // Save & Apply API URL
    document.getElementById("btn-save-api").addEventListener("click", () => {
        const val = apiInput.value.trim() || "http://localhost:8000";
        STATE.apiBaseUrl = val;
        localStorage.setItem("lh_serving_api_url", val);
        refreshDashboardData();
    });

    // Auto refresh controls
    const autoToggle = document.getElementById("auto-refresh-toggle");
    autoToggle.addEventListener("change", () => {
        STATE.autoRefresh = autoToggle.checked;
        updateAutoRefreshTimer();
    });

    const slider = document.getElementById("refresh-slider");
    slider.addEventListener("input", () => {
        STATE.refreshSeconds = Number(slider.value || 5);
        document.getElementById("refresh-sec-label").textContent = `${STATE.refreshSeconds} giây`;
        if (STATE.autoRefresh) updateAutoRefreshTimer();
    });

    document.getElementById("btn-refresh-now").addEventListener("click", () => {
        refreshDashboardData();
    });

    // Chart overlay checkboxes
    document.getElementById("chk-show-vwap").addEventListener("change", (e) => {
        STATE.showVwap = e.target.checked;
        renderMainCandlestickChart();
    });
    document.getElementById("chk-show-spikes").addEventListener("change", (e) => {
        STATE.showSpikes = e.target.checked;
        renderMainCandlestickChart();
    });
    document.getElementById("chk-show-wm").addEventListener("change", (e) => {
        STATE.showWatermark = e.target.checked;
        renderMainCandlestickChart();
    });

    // Detail Tabs
    document.querySelectorAll(".lh-tab-btn").forEach((btn) => {
        btn.addEventListener("click", () => switchTab(btn.dataset.tab));
    });

    // Benchmark Sub-tabs
    document.querySelectorAll("#bench-subnav .lh-subpill").forEach((btn) => {
        btn.addEventListener("click", () => {
            STATE.activeBenchSubtab = btn.dataset.bm;
            document.querySelectorAll("#bench-subnav .lh-subpill").forEach((b) => {
                b.classList.toggle("active", b === btn);
            });
            document.querySelectorAll(".bm-subview").forEach((view) => {
                view.classList.toggle("hidden", view.id !== `bm-view-${STATE.activeBenchSubtab}`);
            });
            renderBenchmarkTab();
        });
    });

    // Filters
    document.getElementById("recon-status-filter").addEventListener("change", renderReconciliationTab);
    document.getElementById("recon-search-input").addEventListener("input", renderReconciliationTab);
    document.getElementById("dq-reason-filter").addEventListener("change", renderDataQualityTab);

    // Start live clock & initial fetch
    startUtcClock();
    refreshDashboardData();
});
