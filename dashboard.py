import argparse
import json
import logging
import math
import os
import queue
import re
import sqlite3
import threading
import time
import tkinter as tk
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import RotatingFileHandler
from tkinter import ttk, scrolledtext, messagebox
from urllib import error as urllib_error
from urllib import request as urllib_request

import serial
from serial import SerialException
from serial.tools import list_ports
import xml.etree.ElementTree as ET


# ============================================================
# Configuration
# ============================================================
COM_PORT = "COM4"
BAUD_RATE = 115200
SERIAL_TIMEOUT = 0.25
SERIAL_RETRY_DELAY_SEC = 3

METER_MAC_ID = "0x0013500500477542"

WINDOW_TITLE = "EMU-2 Energy Dashboard"
WINDOW_SIZE = "1360x960"

# Gauge ranges
MAX_DEMAND_KW = 12.0          # adjust if you want
MAX_PRICE_CENTS = 30.0        # adjust if you want

PRICING_SOURCE_NONE = "No Pricing Source"
PRICING_SOURCE_EMU = "EMU-2 Price"
PRICING_SOURCE_COMED = "ComEd Hourly Pricing"

COMED_FEED_URL = "https://hourlypricing.comed.com/api?type=5minutefeed"
COMED_REFRESH_MS = 60_000

CONFIG_FILE = "dashboard_config.json"
DATA_DIR = "data"
HISTORY_DB_FILE = os.path.join(DATA_DIR, "energy_history.sqlite3")
HISTORY_RETENTION_LIMIT = 5000
LOG_FILE = os.path.join(DATA_DIR, "dashboard.log")


# ============================================================
# Helpers
# ============================================================
def setup_logging() -> logging.Logger:
    os.makedirs(DATA_DIR, exist_ok=True)
    logger = logging.getLogger("emu_dashboard")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(LOG_FILE, maxBytes=512_000, backupCount=3, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


LOGGER = setup_logging()


def parse_hex_int(value: str) -> int:
    value = (value or "").strip()
    if value.lower().startswith("0x"):
        return int(value, 16)
    return int(value)


def scale_value(raw_hex: str, multiplier_hex: str, divisor_hex: str) -> float:
    raw = parse_hex_int(raw_hex)
    multiplier = parse_hex_int(multiplier_hex)
    divisor = parse_hex_int(divisor_hex)
    if divisor == 0:
        return 0.0
    return (raw * multiplier) / divisor


def price_to_dollars(price_hex: str, trailing_digits_hex: str) -> float:
    raw = parse_hex_int(price_hex)
    trailing = parse_hex_int(trailing_digits_hex)
    return raw / (10 ** trailing)


def zigbee_time_to_unix(zigbee_hex: str) -> int:
    """
    Zigbee SEP time is seconds since 2000-01-01 00:00:00 UTC.
    Unix time starts 1970-01-01 00:00:00 UTC.
    Offset = 946684800 seconds.
    """
    raw = parse_hex_int(zigbee_hex)
    return raw + 946684800


def fmt_local_time_from_zigbee(zigbee_hex: str) -> str:
    try:
        unix_ts = zigbee_time_to_unix(zigbee_hex)
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(unix_ts))
    except Exception:
        return str(zigbee_hex)


def fmt_meter_local_time_from_zigbee(zigbee_hex: str) -> str:
    try:
        unix_ts = zigbee_time_to_unix(zigbee_hex)
        # LocalTime from the EMU-2 is already adjusted to local wall time,
        # so display it directly without applying the PC timezone offset again.
        return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(unix_ts))
    except Exception:
        return str(zigbee_hex)


def cents_from_pricecluster(price_hex: str, trailing_digits_hex: str) -> float:
    dollars_per_kwh = price_to_dollars(price_hex, trailing_digits_hex)
    return dollars_per_kwh * 100.0


def strip_serial_prefix(line: str) -> str:
    """
    Turns:
      '16:47:03.027 -> <PriceCluster>'
    into:
      '<PriceCluster>'
    """
    if "->" in line:
        return line.split("->", 1)[1].strip()
    return line.strip()


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def format_link_strength(raw_value: str) -> str:
    if not raw_value:
        return "-"

    try:
        value = parse_hex_int(raw_value)
    except Exception:
        return raw_value

    if value <= 100:
        percent = value
    else:
        percent = round((value / 255.0) * 100.0)

    percent = int(clamp(percent, 0, 100))
    return f"{percent}%"


def load_app_config() -> dict:
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_app_config(data: dict):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
    except Exception:
        pass


def list_serial_port_names() -> list[str]:
    ports = []
    try:
        ports = sorted(port.device for port in list_ports.comports())
    except Exception:
        ports = []
    return ports


def build_web_dashboard_html(port: int) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>EMU-2 Dashboard</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
  <style>
    :root {{
      --bg: #0f172a;
      --card: #111827;
      --border: #243244;
      --text: #f8fafc;
      --muted: #cbd5e1;
    }}
    body {{ background: linear-gradient(180deg, #0f172a 0%, #111827 100%); color: var(--text); }}
    .card, .accordion-item {{ background: rgba(17, 24, 39, 0.96); border: 1px solid var(--border); }}
    .muted, .small {{ color: var(--muted) !important; }}
    .metric-label {{ color: #dbeafe; font-size: .82rem; letter-spacing: .03em; text-transform: uppercase; }}
    .metric-value {{ font-size: clamp(1.35rem, 3vw, 2rem); font-weight: 700; color: #fff; }}
    .status-chip {{ border-radius: 999px; padding: .35rem .75rem; background: #172033; color: #fff; border: 1px solid var(--border); }}
    .chart-wrap {{ height: 320px; }}
    .accordion-button {{ background: #152033; color: #fff; }}
    .accordion-button:not(.collapsed) {{ background: #1b2a44; color: #fff; box-shadow: none; }}
    .accordion-button:focus {{ box-shadow: none; }}
    .accordion-body {{ background: rgba(11, 18, 32, 0.92); color: #f8fafc; }}
    .table-dark {{ --bs-table-bg: #0b1220; --bs-table-color: #f8fafc; --bs-table-border-color: #223147; }}
    code {{ color: #f472b6; }}
  </style>
</head>
<body>
  <div class="container py-3 py-md-4" style="max-width: 1440px;">
    <div class="d-flex flex-column flex-lg-row justify-content-between align-items-lg-center gap-3 mb-4">
      <div>
        <h1 class="h3 mb-1 text-white">EMU-2 Dashboard</h1>
        <div class="muted">Mobile-friendly web view on port {port}</div>
      </div>
      <div class="d-flex flex-wrap gap-2">
        <span class="status-chip" id="portText">Port: --</span>
        <span class="status-chip" id="pricingText">Pricing: --</span>
      </div>
    </div>

    <div class="row g-3 mb-3">
      <div class="col-6 col-xl-3"><div class="card h-100"><div class="card-body"><div class="metric-label">Live Demand</div><div class="metric-value" id="demandValue">--</div></div></div></div>
      <div class="col-6 col-xl-3"><div class="card h-100"><div class="card-body"><div class="metric-label">Current Price</div><div class="metric-value" id="priceValue">--</div></div></div></div>
      <div class="col-6 col-xl-3"><div class="card h-100"><div class="card-body"><div class="metric-label">Current Period Usage</div><div class="metric-value" id="usageValue">--</div></div></div></div>
      <div class="col-6 col-xl-3"><div class="card h-100"><div class="card-body"><div class="metric-label">Estimated Cost / Hour</div><div class="metric-value" id="costValue">--</div></div></div></div>
    </div>

    <div class="card mb-3">
      <div class="card-body d-flex flex-column flex-lg-row justify-content-between gap-3">
        <div>
          <div class="metric-label">System Status</div>
          <div class="fs-5 text-white" id="statusText">Loading...</div>
        </div>
        <div class="row row-cols-2 row-cols-lg-4 g-3 flex-grow-1">
          <div><div class="metric-label">Network</div><div id="networkValue">--</div></div>
          <div><div class="metric-label">Link Strength</div><div id="signalValue">--</div></div>
          <div><div class="metric-label">Last Update</div><div id="updatedValue">--</div></div>
          <div><div class="metric-label">Local Meter Time</div><div id="localTimeValue">--</div></div>
        </div>
      </div>
    </div>

    <div class="card mb-3">
      <div class="card-body">
        <div class="d-flex flex-column flex-lg-row justify-content-between align-items-lg-center mb-3 gap-2">
          <div>
            <h2 class="h5 mb-1 text-white">Recent History</h2>
            <div class="muted">Only confirmed readings are stored in the local SQLite database.</div>
          </div>
        </div>
        <div class="chart-wrap"><canvas id="historyChart"></canvas></div>
      </div>
    </div>

    <div class="accordion" id="infoAccordion">
      <div class="accordion-item mb-3">
        <h2 class="accordion-header">
          <button class="accordion-button" type="button" data-bs-toggle="collapse" data-bs-target="#detailsCollapse" aria-expanded="true">
            Meter Details
          </button>
        </h2>
        <div id="detailsCollapse" class="accordion-collapse collapse show" data-bs-parent="#infoAccordion">
          <div class="accordion-body">
            <div class="row g-3">
              <div class="col-12 col-lg-6"><div class="metric-label">Model / Firmware</div><div id="firmwareValue">--</div></div>
              <div class="col-12 col-lg-6"><div class="metric-label">Source Summary</div><div id="summaryText">--</div></div>
            </div>
          </div>
        </div>
      </div>

      <div class="accordion-item mb-3">
        <h2 class="accordion-header">
          <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse" data-bs-target="#scheduleCollapse" aria-expanded="false">
            Schedule Entries
          </button>
        </h2>
        <div id="scheduleCollapse" class="accordion-collapse collapse" data-bs-parent="#infoAccordion">
          <div class="accordion-body">
            <div class="table-responsive">
              <table class="table table-dark table-sm align-middle mb-0">
                <tbody id="scheduleBody">
                  <tr><td class="muted">No schedule data yet.</td></tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </div>

    </div>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
  <script>
    let chart;

    function ensureChart(labels, priceSeries, usageSeries) {{
      const canvas = document.getElementById('historyChart');
      const ctx = canvas.getContext('2d');
      const safeLabels = Array.isArray(labels) ? labels : [];
      const safePrice = Array.isArray(priceSeries) ? priceSeries.map(Number) : [];
      const safeUsage = Array.isArray(usageSeries) ? usageSeries.map(Number) : [];
      if (!chart) {{
        chart = new Chart(ctx, {{
          type: 'line',
          data: {{
            labels: safeLabels,
            datasets: [
              {{
                label: 'Price (c/kWh)',
                data: safePrice,
                borderColor: '#38bdf8',
                backgroundColor: 'rgba(56, 189, 248, 0.18)',
                tension: 0.25,
                yAxisID: 'y',
                pointRadius: 2,
                borderWidth: 2
              }},
              {{
                label: 'Usage (kWh)',
                data: safeUsage,
                borderColor: '#22c55e',
                backgroundColor: 'rgba(34, 197, 94, 0.18)',
                tension: 0.25,
                yAxisID: 'y1',
                pointRadius: 2,
                borderWidth: 2
              }}
            ]
          }},
          options: {{
            responsive: true,
            maintainAspectRatio: false,
            interaction: {{ mode: 'index', intersect: false }},
            plugins: {{
              legend: {{ labels: {{ color: '#f8fafc' }} }}
            }},
            scales: {{
              x: {{
                type: 'category',
                ticks: {{ color: '#e2e8f0', maxTicksLimit: 8 }},
                grid: {{ color: '#223147' }}
              }},
              y: {{
                type: 'linear',
                position: 'left',
                beginAtZero: false,
                ticks: {{ color: '#7dd3fc' }},
                grid: {{ color: '#223147' }}
              }},
              y1: {{
                type: 'linear',
                position: 'right',
                beginAtZero: false,
                ticks: {{ color: '#86efac' }},
                grid: {{ drawOnChartArea: false }}
              }}
            }}
          }}
        }});
      }} else {{
        chart.data.labels = safeLabels;
        chart.data.datasets[0].data = safePrice;
        chart.data.datasets[1].data = safeUsage;
        chart.update();
      }}
    }}

    function updateSchedule(lines) {{
      const body = document.getElementById('scheduleBody');
      if (!lines.length) {{
        body.innerHTML = '<tr><td class="muted">No schedule data yet.</td></tr>';
        return;
      }}
      body.innerHTML = lines.slice(-8).map(line => `<tr><td>${{line}}</td></tr>`).join('');
    }}

    function applySnapshot(snapshot) {{
      document.getElementById('statusText').textContent = snapshot.status_text || '--';
      document.getElementById('portText').textContent = `Port: ${{snapshot.com_port || '--'}}`;
      document.getElementById('pricingText').textContent = `Pricing: ${{snapshot.pricing_source || '--'}}`;
      document.getElementById('demandValue').textContent = `${{snapshot.demand_kw.toFixed(3)}} kW`;
      document.getElementById('priceValue').textContent = `${{snapshot.price_cents.toFixed(2)}} c/kWh`;
      document.getElementById('usageValue').textContent = `${{snapshot.current_period_kwh.toFixed(3)}} kWh`;
      document.getElementById('costValue').textContent = `$${{snapshot.cost_per_hour.toFixed(2)}}/hr`;
      document.getElementById('networkValue').textContent = snapshot.network_status || '--';
      document.getElementById('signalValue').textContent = snapshot.link_strength || '--';
      document.getElementById('updatedValue').textContent = snapshot.last_update || '--';
      document.getElementById('localTimeValue').textContent = snapshot.local_time || '--';
      document.getElementById('firmwareValue').textContent = snapshot.firmware || '--';
      document.getElementById('summaryText').textContent = `${{snapshot.com_port || '--'}} | ${{snapshot.pricing_source || '--'}} | ${{snapshot.price_cents.toFixed(2)}} c/kWh`;
      updateSchedule(snapshot.schedule_lines || []);
    }}

    async function refreshData() {{
      const [snapshotResp, historyResp] = await Promise.all([
        fetch('/api/snapshot'),
        fetch('/api/history')
      ]);
      const snapshot = await snapshotResp.json();
      const history = await historyResp.json();
      applySnapshot(snapshot);
      ensureChart(history.labels, history.price_cents, history.usage_kwh);
    }}

    refreshData();
    setInterval(refreshData, 5000);
  </script>
</body>
</html>"""


class DashboardWebServer:
    def __init__(self, provider, port: int):
        self.provider = provider
        self.port = port
        self.httpd = None
        self.thread = None

    def start(self):
        provider = self.provider
        port = self.port

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/":
                    body = build_web_dashboard_html(port).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                if self.path == "/api/snapshot":
                    body = json.dumps(provider.get_snapshot()).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                if self.path == "/api/history":
                    body = json.dumps(provider.get_history_payload()).encode("utf-8")
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return

                self.send_error(404)

            def log_message(self, format, *args):
                return

        self.httpd = ThreadingHTTPServer(("0.0.0.0", self.port), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def stop(self):
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()
            self.httpd = None


# ============================================================
# Gauge Widget
# ============================================================
class SemiGauge(tk.Canvas):
    def __init__(
        self,
        master,
        width=320,
        height=210,
        min_value=0,
        max_value=100,
        title="Gauge",
        units="",
        **kwargs
    ):
        super().__init__(
            master,
            width=width,
            height=height,
            bg="#111827",
            highlightthickness=0,
            **kwargs
        )

        self.width = width
        self.height = height
        self.min_value = min_value
        self.max_value = max_value
        self.title = title
        self.units = units
        self.value = 0.0

        self.pad = 24
        self.arc_width = 18

        self.draw_static()
        self.update_value(0.0)

    def draw_static(self):
        self.delete("all")

        cx = self.width / 2
        cy = self.height - 24
        r = min(self.width / 2 - self.pad, self.height - 48)

        x0 = cx - r
        y0 = cy - r
        x1 = cx + r
        y1 = cy + r

        # Title
        self.create_text(
            self.width / 2,
            22,
            text=self.title,
            fill="#E5E7EB",
            font=("Segoe UI", 16, "bold")
        )

        # Background arc
        self.create_arc(
            x0, y0, x1, y1,
            start=180,
            extent=180,
            style="arc",
            width=self.arc_width,
            outline="#374151"
        )

        # Tick marks + labels
        tick_count = 6
        for i in range(tick_count + 1):
            frac = i / tick_count
            angle_deg = 180 - (180 * frac)
            angle_rad = math.radians(angle_deg)

            outer_r = r + 2
            inner_r = r - 18

            x_outer = cx + outer_r * math.cos(angle_rad)
            y_outer = cy - outer_r * math.sin(angle_rad)
            x_inner = cx + inner_r * math.cos(angle_rad)
            y_inner = cy - inner_r * math.sin(angle_rad)

            self.create_line(
                x_inner, y_inner, x_outer, y_outer,
                fill="#9CA3AF",
                width=2
            )

            label_val = self.min_value + (self.max_value - self.min_value) * frac
            label_r = r - 36
            lx = cx + label_r * math.cos(angle_rad)
            ly = cy - label_r * math.sin(angle_rad)

            self.create_text(
                lx, ly,
                text=f"{label_val:.0f}",
                fill="#D1D5DB",
                font=("Segoe UI", 9)
            )

        # Dynamic arc, needle, value
        self.dynamic_arc = self.create_arc(
            x0, y0, x1, y1,
            start=180,
            extent=0,
            style="arc",
            width=self.arc_width,
            outline="#60A5FA"
        )

        self.needle = self.create_line(
            cx, cy,
            cx, cy - r + 28,
            fill="#F9FAFB",
            width=4
        )

        self.create_oval(
            cx - 8, cy - 8, cx + 8, cy + 8,
            fill="#F9FAFB",
            outline=""
        )

        self.value_text = self.create_text(
            self.width / 2,
            self.height - 70,
            text="0.0",
            fill="#F9FAFB",
            font=("Segoe UI", 26, "bold")
        )

        self.units_text = self.create_text(
            self.width / 2,
            self.height - 42,
            text=self.units,
            fill="#9CA3AF",
            font=("Segoe UI", 12)
        )

    def update_value(self, value: float):
        self.value = clamp(value, self.min_value, self.max_value)
        frac = 0.0
        if self.max_value != self.min_value:
            frac = (self.value - self.min_value) / (self.max_value - self.min_value)
        frac = clamp(frac, 0.0, 1.0)

        # Arc extent
        extent = 180 * frac
        self.itemconfig(self.dynamic_arc, extent=extent)

        # Needle angle
        cx = self.width / 2
        cy = self.height - 24
        r = min(self.width / 2 - self.pad, self.height - 48) - 26

        angle_deg = 180 - (180 * frac)
        angle_rad = math.radians(angle_deg)

        nx = cx + r * math.cos(angle_rad)
        ny = cy - r * math.sin(angle_rad)
        self.coords(self.needle, cx, cy, nx, ny)

        # Color zones
        if frac < 0.5:
            color = "#22C55E"
        elif frac < 0.8:
            color = "#F59E0B"
        else:
            color = "#EF4444"

        self.itemconfig(self.dynamic_arc, outline=color)

        if "¢" in self.units:
            display = f"{self.value:.2f}"
        else:
            display = f"{self.value:.3f}"

        self.itemconfig(self.value_text, text=display)


# ============================================================
# Serial Worker
# ============================================================
class EmuSerialWorker(threading.Thread):
    def __init__(self, port, baud, meter_mac, out_queue):
        super().__init__(daemon=True)
        self.port = port
        self.baud = baud
        self.meter_mac = meter_mac
        self.out_queue = out_queue

        self.serial_conn = None
        self.stop_event = threading.Event()
        self.write_lock = threading.Lock()

        self.current_root = None
        self.current_lines = []

    def run(self):
        while not self.stop_event.is_set():
            try:
                if not self.serial_conn or not self.serial_conn.is_open:
                    self.current_root = None
                    self.current_lines = []
                    self.out_queue.put(("status", f"Connecting to {self.port}..."))
                    self.serial_conn = serial.Serial(
                        self.port,
                        self.baud,
                        timeout=SERIAL_TIMEOUT
                    )
                    self.out_queue.put(("connected", f"Connected to {self.port} @ {self.baud}"))

                line = self.serial_conn.readline()
                if not line:
                    continue

                text = line.decode("utf-8", errors="replace").strip()
                cleaned = strip_serial_prefix(text)

                if cleaned:
                    self.out_queue.put(("raw", cleaned))
                    self._feed_xml_line(cleaned)

            except SerialException as exc:
                self._close_serial()
                if self.stop_event.is_set():
                    break
                self.out_queue.put(
                    ("error", f"Could not open {self.port}: {exc}. Retrying in {SERIAL_RETRY_DELAY_SEC} seconds.")
                )
                time.sleep(SERIAL_RETRY_DELAY_SEC)
            except Exception as exc:
                self._close_serial()
                if self.stop_event.is_set():
                    break
                self.out_queue.put(
                    ("error", f"Serial read error: {exc}. Retrying in {SERIAL_RETRY_DELAY_SEC} seconds.")
                )
                time.sleep(SERIAL_RETRY_DELAY_SEC)

        self._close_serial()
        self.out_queue.put(("status", "Disconnected"))

    def stop(self):
        self.stop_event.set()
        self._close_serial()

    def _close_serial(self):
        try:
            if self.serial_conn and self.serial_conn.is_open:
                self.serial_conn.close()
        except Exception:
            pass
        self.serial_conn = None

    def send_xml(self, xml_text: str):
        if not self.serial_conn or not self.serial_conn.is_open:
            self.out_queue.put(("error", "Serial port not connected"))
            return

        with self.write_lock:
            try:
                payload = xml_text.strip() + "\r\n"
                self.serial_conn.write(payload.encode("utf-8"))
                self.serial_conn.flush()
                self.out_queue.put(("sent", xml_text.strip()))
            except Exception as exc:
                self.out_queue.put(("error", f"Serial write error: {exc}"))

    def send_command(self, name: str, include_meter=True, refresh=None, extra_tags=None):
        parts = [
            "<Command>",
            f"  <Name>{name}</Name>"
        ]

        if include_meter:
            parts.append(f"  <MeterMacId>{self.meter_mac}</MeterMacId>")

        if refresh is not None:
            parts.append(f"  <Refresh>{refresh}</Refresh>")

        if extra_tags:
            for tag, value in extra_tags.items():
                parts.append(f"  <{tag}>{value}</{tag}>")

        parts.append("</Command>")

        self.send_xml("\n".join(parts))

    def _feed_xml_line(self, line: str):
        if self.current_root is None:
            if line.startswith("<") and not line.startswith("</"):
                m = re.match(r"<([A-Za-z0-9_]+)>", line)
                if m:
                    self.current_root = m.group(1)
                    self.current_lines = [line]
            return

        self.current_lines.append(line)

        if line == f"</{self.current_root}>":
            xml_text = "\n".join(self.current_lines)
            self.current_root = None
            self.current_lines = []

            try:
                elem = ET.fromstring(xml_text)
                self.out_queue.put(("xml", elem))
            except ET.ParseError:
                self.out_queue.put(("error", f"XML parse error for block:\n{xml_text}"))


# ============================================================
# Main App
# ============================================================
class EmuDashboardApp:
    def __init__(self, root, web_port=8000):
        self.root = root
        self.web_port = web_port
        self.root.title(WINDOW_TITLE)
        self.root.geometry(WINDOW_SIZE)
        self.root.configure(bg="#0F172A")

        self.queue = queue.Queue()
        self.worker = None
        self.config_data = load_app_config()
        preferred_port = self.config_data.get("preferred_com_port") or COM_PORT
        self.com_port_var = tk.StringVar(value=preferred_port)
        self.available_ports = []
        self.history_lock = threading.Lock()
        self.history_conn = self.init_history_store()
        self.last_history_signature = None
        self.last_history_save_time = ""
        self.web_server = DashboardWebServer(self, self.web_port)

        self.data = {
            "device_mac": "",
            "meter_mac": METER_MAC_ID,
            "utc_time": "",
            "local_time": "",
            "demand_kw": 0.0,
            "demand_known": False,
            "price_cents": 0.0,
            "emu_price_cents": 0.0,
            "emu_price_known": False,
            "comed_price_cents": 0.0,
            "comed_price_known": False,
            "current_period_kwh": 0.0,
            "current_period_known": False,
            "lifetime_kwh": 0.0,
            "summation_received_kwh": 0.0,
            "link_strength": "",
            "network_status": "",
            "fw_version": "",
            "model_id": "",
            "last_update": "",
            "price_start": "",
            "price_duration_min": "",
            "comed_price_time": "",
            "schedule_lines": [],
        }

        preferred_pricing_source = self.config_data.get("preferred_pricing_source") or PRICING_SOURCE_NONE
        if preferred_pricing_source not in [PRICING_SOURCE_NONE, PRICING_SOURCE_EMU, PRICING_SOURCE_COMED]:
            preferred_pricing_source = PRICING_SOURCE_NONE
        self.pricing_source_var = tk.StringVar(value=preferred_pricing_source)
        self.comed_fetch_in_progress = False

        self._build_ui()
        self._start_serial()
        self.refresh_ui()
        self.refresh_history_chart()
        if self.pricing_source_var.get() == PRICING_SOURCE_COMED:
            self.fetch_comed_price()
        self.web_server.start()

        self.root.after(100, self.process_queue)
        self.root.after(COMED_REFRESH_MS, self.schedule_comed_refresh)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build_ui(self):
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure("TFrame", background="#0F172A")
        style.configure("Card.TFrame", background="#111827")
        style.configure("TLabel", background="#0F172A", foreground="#F9FAFB", font=("Segoe UI", 11))
        style.configure("Header.TLabel", background="#0F172A", foreground="#F9FAFB", font=("Segoe UI", 20, "bold"))
        style.configure("Sub.TLabel", background="#0F172A", foreground="#94A3B8", font=("Segoe UI", 10))
        style.configure("CardLabel.TLabel", background="#111827", foreground="#E5E7EB", font=("Segoe UI", 10))
        style.configure("CardValue.TLabel", background="#111827", foreground="#F9FAFB", font=("Segoe UI", 18, "bold"))
        style.configure("TButton", font=("Segoe UI", 10))
        style.configure("Dashboard.TCombobox", fieldbackground="#F8FAFC", background="#F8FAFC", foreground="#111827", arrowcolor="#111827")
        style.map(
            "Dashboard.TCombobox",
            fieldbackground=[("readonly", "#F8FAFC")],
            foreground=[("readonly", "#111827")],
            selectforeground=[("readonly", "#111827")],
            selectbackground=[("readonly", "#E2E8F0")]
        )

        outer = ttk.Frame(self.root)
        outer.pack(fill="both", expand=True, padx=14, pady=14)

        # Header
        header = ttk.Frame(outer)
        header.pack(fill="x", pady=(0, 10))

        ttk.Label(header, text="EMU-2 Energy Dashboard", style="Header.TLabel").pack(side="left")

        self.status_var = tk.StringVar(value="Starting...")
        ttk.Label(header, textvariable=self.status_var, style="Sub.TLabel").pack(side="right")

        connection_row = ttk.Frame(outer)
        connection_row.pack(fill="x", pady=(0, 10))

        ttk.Label(connection_row, text="COM Port", style="Sub.TLabel").pack(side="left", padx=(0, 8))
        self.com_port_combo = ttk.Combobox(
            connection_row,
            textvariable=self.com_port_var,
            state="readonly",
            width=12,
            style="Dashboard.TCombobox"
        )
        self.com_port_combo.pack(side="left")
        self.com_port_combo.bind("<<ComboboxSelected>>", self.on_com_port_changed)
        ttk.Button(connection_row, text="Refresh Ports", command=self.refresh_com_ports).pack(side="left", padx=(8, 0))

        # Top section
        top = ttk.Frame(outer)
        top.pack(fill="x", pady=(0, 12))

        self.demand_gauge = SemiGauge(
            top,
            title="Live Demand",
            units="kW",
            min_value=0,
            max_value=MAX_DEMAND_KW
        )
        self.demand_gauge.pack(side="left", padx=(0, 12))

        self.price_gauge = SemiGauge(
            top,
            title="Current Price",
            units="¢/kWh",
            min_value=0,
            max_value=MAX_PRICE_CENTS
        )
        self.price_gauge.pack(side="left", padx=(0, 12))

        right_top = ttk.Frame(top)
        right_top.pack(side="left", fill="both", expand=True)

        info_grid = ttk.Frame(right_top)
        info_grid.pack(fill="x")

        self.cards = {}

        card_items = [
            ("Current Period Usage", "current_period", "0.000 kWh"),
            ("Lifetime Delivered", "lifetime", "0.000 kWh"),
            ("Estimated Cost / Hour", "cost_hour", "$0.00/hr"),
            ("Network Status", "network", "-"),
            ("Link Strength", "signal", "-"),
            ("Last Update", "updated", "-"),
            ("Local Meter Time", "localtime", "-"),
            ("Model / Firmware", "firmware", "-"),
        ]

        for i, (label_text, key, initial) in enumerate(card_items):
            frame = ttk.Frame(info_grid, style="Card.TFrame")
            frame.grid(row=i // 2, column=i % 2, sticky="nsew", padx=6, pady=6)

            info_grid.columnconfigure(i % 2, weight=1, minsize=285)

            ttk.Label(frame, text=label_text, style="CardLabel.TLabel").pack(anchor="w", padx=12, pady=(10, 4))
            var = tk.StringVar(value=initial)
            ttk.Label(frame, textvariable=var, style="CardValue.TLabel").pack(anchor="w", padx=12, pady=(0, 12))
            self.cards[key] = var

        # Command buttons
        command_frame = ttk.Frame(right_top, style="Card.TFrame")
        command_frame.pack(fill="x", padx=6, pady=(10, 6))

        ttk.Label(command_frame, text="Query Commands", style="CardLabel.TLabel").pack(anchor="w", padx=12, pady=(10, 8))

        source_row = ttk.Frame(command_frame)
        source_row.pack(fill="x", padx=12, pady=(0, 8))

        ttk.Label(source_row, text="Pricing Source", style="CardLabel.TLabel").pack(side="left", padx=(0, 8))
        pricing_source = ttk.Combobox(
            source_row,
            textvariable=self.pricing_source_var,
            values=[PRICING_SOURCE_NONE, PRICING_SOURCE_EMU, PRICING_SOURCE_COMED],
            state="readonly",
            width=22,
            style="Dashboard.TCombobox"
        )
        pricing_source.pack(side="left")
        pricing_source.bind("<<ComboboxSelected>>", self.on_pricing_source_changed)

        btn_grid = ttk.Frame(command_frame)
        btn_grid.pack(fill="x", padx=10, pady=(0, 10))

        buttons = [
            ("Demand", lambda: self.send_named_command("get_instantaneous_demand", refresh="Y")),
            ("Price", lambda: self.send_named_command("get_current_price", refresh="Y")),
            ("Summation", lambda: self.send_named_command("get_current_summation_delivered", refresh="Y")),
            ("Current Period", lambda: self.send_named_command("get_current_period_usage")),
            ("Time", lambda: self.send_named_command("get_time")),
            ("Meter Info", lambda: self.send_named_command("get_meter_info")),
            ("Network Info", self.send_network_info),
            ("Schedule", self.send_schedule),
        ]

        for idx, (text, cmd) in enumerate(buttons):
            ttk.Button(btn_grid, text=text, command=cmd).grid(
                row=idx // 4,
                column=idx % 4,
                padx=4,
                pady=4,
                sticky="ew"
            )
            btn_grid.columnconfigure(idx % 4, weight=1)

        # Bottom area
        bottom = ttk.Frame(outer)
        bottom.pack(fill="both", expand=True)

        left_bottom = ttk.Frame(bottom)
        left_bottom.pack(side="left", fill="both", expand=True, padx=(0, 8))

        right_bottom = ttk.Frame(bottom)
        right_bottom.pack(side="left", fill="both", expand=True)

        history_card = ttk.Frame(left_bottom, style="Card.TFrame")
        history_card.pack(fill="x", pady=(0, 8))

        ttk.Label(history_card, text="Recent History", style="CardLabel.TLabel").pack(anchor="w", padx=12, pady=(10, 6))

        self.history_canvas = tk.Canvas(
            history_card,
            height=240,
            bg="#0B1220",
            highlightthickness=0
        )
        self.history_canvas.pack(fill="x", padx=12, pady=(0, 12))

        # Schedule panel
        schedule_card = ttk.Frame(left_bottom, style="Card.TFrame")
        schedule_card.pack(fill="both", expand=True, pady=(0, 8))

        ttk.Label(schedule_card, text="Schedules", style="CardLabel.TLabel").pack(anchor="w", padx=12, pady=(10, 6))

        self.schedule_text = scrolledtext.ScrolledText(
            schedule_card,
            height=10,
            bg="#0B1220",
            fg="#E5E7EB",
            insertbackground="#E5E7EB",
            relief="flat",
            font=("Consolas", 10)
        )
        self.schedule_text.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        self.schedule_text.config(state="disabled")

        # Raw log panel
        raw_card = ttk.Frame(right_bottom, style="Card.TFrame")
        raw_card.pack(fill="both", expand=True)

        ttk.Label(raw_card, text="Raw XML Log", style="CardLabel.TLabel").pack(anchor="w", padx=12, pady=(10, 6))

        self.raw_text = scrolledtext.ScrolledText(
            raw_card,
            bg="#0B1220",
            fg="#E5E7EB",
            insertbackground="#E5E7EB",
            relief="flat",
            font=("Consolas", 10)
        )
        self.raw_text.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        self.raw_text.config(state="disabled")
        self.refresh_com_ports()

    def _start_serial(self):
        self.stop_serial_worker()
        self.worker = EmuSerialWorker(self.com_port_var.get(), BAUD_RATE, METER_MAC_ID, self.queue)
        self.worker.start()
        self.status_var.set(f"Waiting for {self.com_port_var.get()}...")

    def stop_serial_worker(self):
        if self.worker:
            self.worker.stop()
            self.worker = None

    def schedule_initial_queries(self):
        self.root.after(1200, self.send_network_info)
        self.root.after(1800, lambda: self.send_named_command("get_device_info", include_meter=False))
        self.root.after(2400, lambda: self.send_named_command("get_time"))
        self.root.after(3000, lambda: self.send_named_command("get_current_price", refresh="Y"))
        self.root.after(3600, lambda: self.send_named_command("get_instantaneous_demand", refresh="Y"))
        self.root.after(4200, lambda: self.send_named_command("get_current_summation_delivered", refresh="Y"))
        self.root.after(4800, lambda: self.send_named_command("get_current_period_usage"))
        self.root.after(5400, self.send_schedule)

    def refresh_com_ports(self):
        self.available_ports = list_serial_port_names()

        values = list(self.available_ports)
        current = self.com_port_var.get()
        if current and current not in values:
            values.append(current)

        if not values:
            values = [current or COM_PORT]

        self.com_port_combo["values"] = values
        if not self.com_port_var.get():
            self.com_port_var.set(values[0])

    def on_com_port_changed(self, _event=None):
        selected_port = self.com_port_var.get()
        self.config_data["preferred_com_port"] = selected_port
        save_app_config(self.config_data)
        self.append_raw(f"[STATUS] Preferred COM port set to {selected_port}")
        self._start_serial()

    def send_named_command(self, name, include_meter=True, refresh=None, extra_tags=None):
        if self.worker:
            self.worker.send_command(
                name=name,
                include_meter=include_meter,
                refresh=refresh,
                extra_tags=extra_tags
            )

    def send_network_info(self):
        if self.worker:
            self.worker.send_xml(
                "<Command>\n"
                "  <Name>get_network_info</Name>\n"
                "</Command>"
            )

    def send_schedule(self):
        if self.worker:
            self.worker.send_xml(
                "<Command>\n"
                "  <Name>get_schedule</Name>\n"
                "</Command>"
            )

    def init_history_store(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        conn = sqlite3.connect(HISTORY_DB_FILE, check_same_thread=False)
        with self.history_lock:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS readings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sample_time TEXT NOT NULL,
                    pricing_source TEXT NOT NULL,
                    active_price_cents REAL NOT NULL,
                    emu_price_cents REAL NOT NULL,
                    comed_price_cents REAL NOT NULL,
                    demand_kw REAL NOT NULL,
                    current_period_kwh REAL NOT NULL,
                    local_meter_time TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_readings_sample_time ON readings(sample_time)"
            )
            conn.execute(
                """
                DELETE FROM readings
                WHERE active_price_cents = 0
                  AND demand_kw = 0
                  AND current_period_kwh = 0
                  AND COALESCE(local_meter_time, '') = ''
                """
            )
            conn.commit()
        return conn

    def maybe_record_history(self):
        if not (
            self.data["demand_known"]
            and self.data["current_period_known"]
            and self.get_active_price_known()
        ):
            return

        sample_time = time.strftime("%Y-%m-%d %H:%M:%S")
        signature = (
            self.pricing_source_var.get(),
            round(self.data["price_cents"], 4),
            round(self.data["emu_price_cents"], 4),
            round(self.data["comed_price_cents"], 4),
            round(self.data["demand_kw"], 4),
            round(self.data["current_period_kwh"], 4),
            self.data["local_time"],
        )

        if signature == self.last_history_signature:
            return

        self.last_history_signature = signature
        self.last_history_save_time = sample_time

        try:
            with self.history_lock:
                self.history_conn.execute(
                    """
                    INSERT INTO readings (
                        sample_time,
                        pricing_source,
                        active_price_cents,
                        emu_price_cents,
                        comed_price_cents,
                        demand_kw,
                        current_period_kwh,
                        local_meter_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        sample_time,
                        self.pricing_source_var.get(),
                        self.data["price_cents"],
                        self.data["emu_price_cents"],
                        self.data["comed_price_cents"],
                        self.data["demand_kw"],
                        self.data["current_period_kwh"],
                        self.data["local_time"],
                    ),
                )
                self.history_conn.execute(
                    """
                    DELETE FROM readings
                    WHERE id NOT IN (
                        SELECT id FROM readings
                        ORDER BY sample_time DESC, id DESC
                        LIMIT ?
                    )
                    """,
                    (HISTORY_RETENTION_LIMIT,),
                )
                self.history_conn.commit()
        except Exception as exc:
            self.append_raw(f"[ERROR] History save failed: {exc}")
            return

        self.refresh_history_chart()

    def load_recent_history(self, limit=240):
        try:
            with self.history_lock:
                rows = self.history_conn.execute(
                    """
                    SELECT sample_time, active_price_cents, current_period_kwh
                    FROM readings
                    ORDER BY sample_time DESC, id DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        except Exception as exc:
            self.append_raw(f"[ERROR] History load failed: {exc}")
            return []

        return list(reversed(rows))

    def get_snapshot(self):
        fw = self.data["fw_version"]
        model = self.data["model_id"]
        firmware = f"{model} / {fw}" if (fw or model) else "-"
        price_cents = self.get_active_price_cents()

        return {
            "com_port": self.com_port_var.get(),
            "pricing_source": self.pricing_source_var.get(),
            "demand_kw": float(self.data["demand_kw"]),
            "price_cents": float(price_cents),
            "current_period_kwh": float(self.data["current_period_kwh"]),
            "lifetime_kwh": float(self.data["lifetime_kwh"]),
            "cost_per_hour": float(self.data["demand_kw"] * (price_cents / 100.0)),
            "network_status": self.data["network_status"] or "-",
            "link_strength": format_link_strength(self.data["link_strength"]),
            "last_update": self.data["last_update"] or "-",
            "local_time": self.data["local_time"] or "-",
            "firmware": firmware,
            "schedule_lines": list(self.data["schedule_lines"]),
            "status_text": self.status_var.get(),
        }

    def get_history_payload(self, limit=240):
        rows = self.load_recent_history(limit=limit)
        return {
            "labels": [row[0][11:16] for row in rows],
            "price_cents": [float(row[1]) for row in rows],
            "usage_kwh": [float(row[2]) for row in rows],
        }

    def get_active_price_known(self) -> bool:
        source = self.pricing_source_var.get()
        if source == PRICING_SOURCE_EMU:
            return self.data["emu_price_known"]
        if source == PRICING_SOURCE_COMED:
            return self.data["comed_price_known"]
        return False

    def refresh_history_chart(self):
        if not hasattr(self, "history_canvas"):
            return

        canvas = self.history_canvas
        canvas.update_idletasks()
        width = max(canvas.winfo_width(), 320)
        height = max(canvas.winfo_height(), 240)
        canvas.delete("all")

        rows = self.load_recent_history()
        if len(rows) < 2:
            canvas.create_text(
                width / 2,
                height / 2,
                text="History will appear here after a few samples are saved.",
                fill="#94A3B8",
                font=("Segoe UI", 11)
            )
            return

        pad_left = 48
        pad_right = 18
        pad_top = 20
        pad_bottom = 34
        plot_width = width - pad_left - pad_right
        plot_height = height - pad_top - pad_bottom

        canvas.create_rectangle(
            pad_left,
            pad_top,
            pad_left + plot_width,
            pad_top + plot_height,
            outline="#334155"
        )

        price_values = [row[1] for row in rows]
        usage_values = [row[2] for row in rows]

        price_min = min(price_values)
        price_max = max(price_values)
        usage_min = min(usage_values)
        usage_max = max(usage_values)

        if price_min == price_max:
            price_min -= 1.0
            price_max += 1.0
        if usage_min == usage_max:
            usage_min -= 0.1
            usage_max += 0.1

        for frac in (0.0, 0.5, 1.0):
            y = pad_top + plot_height - (plot_height * frac)
            canvas.create_line(pad_left, y, pad_left + plot_width, y, fill="#1E293B")

        def series_points(values, low, high):
            points = []
            span = max(high - low, 0.0001)
            count = max(len(values) - 1, 1)
            for idx, value in enumerate(values):
                x = pad_left + (plot_width * idx / count)
                y = pad_top + plot_height - (((value - low) / span) * plot_height)
                points.extend([x, y])
            return points

        price_points = series_points(price_values, price_min, price_max)
        usage_points = series_points(usage_values, usage_min, usage_max)

        canvas.create_line(*price_points, fill="#38BDF8", width=2, smooth=True)
        canvas.create_line(*usage_points, fill="#22C55E", width=2, smooth=True)

        canvas.create_text(
            pad_left,
            height - 14,
            text=rows[0][0][11:16],
            anchor="w",
            fill="#94A3B8",
            font=("Segoe UI", 9)
        )
        canvas.create_text(
            pad_left + plot_width,
            height - 14,
            text=rows[-1][0][11:16],
            anchor="e",
            fill="#94A3B8",
            font=("Segoe UI", 9)
        )

        canvas.create_text(
            pad_left,
            8,
            text=f"Price {price_values[-1]:.2f} ¢/kWh",
            anchor="w",
            fill="#38BDF8",
            font=("Segoe UI", 10, "bold")
        )
        canvas.create_text(
            width - pad_right,
            8,
            text=f"Usage {usage_values[-1]:.3f} kWh",
            anchor="e",
            fill="#22C55E",
            font=("Segoe UI", 10, "bold")
        )

    def append_raw(self, text: str):
        self.raw_text.config(state="normal")
        self.raw_text.insert("end", text + "\n")
        self.raw_text.see("end")
        self.raw_text.config(state="disabled")

    def set_schedule_text(self):
        self.schedule_text.config(state="normal")
        self.schedule_text.delete("1.0", "end")
        for line in self.data["schedule_lines"]:
            self.schedule_text.insert("end", line + "\n")
        self.schedule_text.config(state="disabled")

    def process_queue(self):
        try:
            while True:
                msg_type, payload = self.queue.get_nowait()

                if msg_type == "status":
                    self.status_var.set(payload)
                    self.append_raw(f"[STATUS] {payload}")

                elif msg_type == "connected":
                    self.status_var.set(payload)
                    self.append_raw(f"[STATUS] {payload}")
                    self.schedule_initial_queries()

                elif msg_type == "error":
                    self.status_var.set(payload)
                    self.append_raw(f"[ERROR] {payload}")

                elif msg_type == "sent":
                    self.append_raw(f"[SENT]\n{payload}\n")

                elif msg_type == "raw":
                    self.append_raw(payload)

                elif msg_type == "xml":
                    self.handle_xml(payload)
                    self.refresh_ui()
                    self.maybe_record_history()

                elif msg_type == "comed_price":
                    self.comed_fetch_in_progress = False
                    price_cents, price_time = payload
                    self.data["comed_price_cents"] = price_cents
                    self.data["comed_price_known"] = True
                    self.data["comed_price_time"] = price_time
                    self.data["last_update"] = price_time
                    LOGGER.info("ComEd price updated to %.4f c/kWh at %s", price_cents, price_time)
                    self.refresh_ui()
                    self.maybe_record_history()

                elif msg_type == "comed_error":
                    self.comed_fetch_in_progress = False
                    LOGGER.error("%s", payload)
                    self.append_raw(f"[ERROR] {payload}")
                    self.refresh_ui()

        except queue.Empty:
            pass

        self.root.after(100, self.process_queue)

    def handle_xml(self, elem: ET.Element):
        tag = elem.tag
        self.data["last_update"] = time.strftime("%Y-%m-%d %H:%M:%S")

        def txt(name, default=""):
            child = elem.find(name)
            return child.text.strip() if child is not None and child.text is not None else default

        if tag == "DeviceInfo":
            self.data["device_mac"] = txt("DeviceMacId")
            self.data["fw_version"] = txt("FWVersion")
            self.data["model_id"] = txt("ModelId")

        elif tag == "NetworkInfo":
            self.data["device_mac"] = txt("DeviceMacId")
            self.data["meter_mac"] = txt("CoordMacId") or txt("MeterMacId") or self.data["meter_mac"]
            self.data["network_status"] = txt("Status")
            self.data["link_strength"] = txt("LinkStrength")

        elif tag == "TimeCluster":
            self.data["utc_time"] = fmt_local_time_from_zigbee(txt("UTCTime"))
            self.data["local_time"] = fmt_meter_local_time_from_zigbee(txt("LocalTime"))

        elif tag == "InstantaneousDemand":
            self.data["device_mac"] = txt("DeviceMacId")
            self.data["meter_mac"] = txt("MeterMacId") or self.data["meter_mac"]
            self.data["demand_kw"] = scale_value(
                txt("Demand"),
                txt("Multiplier"),
                txt("Divisor")
            )
            self.data["demand_known"] = True

        elif tag == "PriceCluster":
            self.data["device_mac"] = txt("DeviceMacId")
            self.data["meter_mac"] = txt("MeterMacId") or self.data["meter_mac"]
            self.data["emu_price_cents"] = cents_from_pricecluster(
                txt("Price"),
                txt("TrailingDigits")
            )
            self.data["emu_price_known"] = True
            self.data["price_start"] = fmt_local_time_from_zigbee(txt("StartTime"))
            try:
                duration_minutes = parse_hex_int(txt("Duration"))
                self.data["price_duration_min"] = str(duration_minutes)
            except Exception:
                self.data["price_duration_min"] = ""

        elif tag == "CurrentSummationDelivered":
            self.data["device_mac"] = txt("DeviceMacId")
            self.data["meter_mac"] = txt("MeterMacId") or self.data["meter_mac"]
            self.data["lifetime_kwh"] = scale_value(
                txt("SummationDelivered"),
                txt("Multiplier"),
                txt("Divisor")
            )
            self.data["summation_received_kwh"] = scale_value(
                txt("SummationReceived"),
                txt("Multiplier"),
                txt("Divisor")
            )

        elif tag == "CurrentPeriodUsage":
            self.data["device_mac"] = txt("DeviceMacId")
            self.data["meter_mac"] = txt("MeterMacId") or self.data["meter_mac"]
            self.data["current_period_kwh"] = scale_value(
                txt("CurrentUsage"),
                txt("Multiplier"),
                txt("Divisor")
            )
            self.data["current_period_known"] = True

        elif tag == "ScheduleInfo":
            line = (
                f"Mode={txt('Mode'):>5}   "
                f"Event={txt('Event'):<18}   "
                f"Frequency={parse_hex_int(txt('Frequency')) if txt('Frequency') else 0:>4}s   "
                f"Enabled={txt('Enabled')}"
            )
            self.data["schedule_lines"].append(line)

            # keep unique-ish without uncontrolled growth
            deduped = []
            seen = set()
            for item in self.data["schedule_lines"]:
                if item not in seen:
                    deduped.append(item)
                    seen.add(item)
            self.data["schedule_lines"] = deduped[-20:]

        elif tag == "Warning":
            self.append_raw(f"[WARNING] {txt('Text')}")

    def refresh_ui(self):
        demand_kw = self.data["demand_kw"]
        price_cents = self.get_active_price_cents()
        self.data["price_cents"] = price_cents
        current_period_kwh = self.data["current_period_kwh"]
        lifetime_kwh = self.data["lifetime_kwh"]

        cost_per_hour = demand_kw * (price_cents / 100.0)

        self.demand_gauge.update_value(demand_kw)
        self.price_gauge.update_value(price_cents)

        self.cards["current_period"].set(f"{current_period_kwh:,.3f} kWh")
        self.cards["lifetime"].set(f"{lifetime_kwh:,.3f} kWh")
        self.cards["cost_hour"].set(f"${cost_per_hour:,.2f}/hr")
        self.cards["network"].set(self.data["network_status"] or "-")
        self.cards["signal"].set(format_link_strength(self.data["link_strength"]))
        self.cards["updated"].set(self.data["last_update"] or "-")
        self.cards["localtime"].set(self.data["local_time"] or "-")

        fw = self.data["fw_version"]
        model = self.data["model_id"]
        if fw or model:
            self.cards["firmware"].set(f"{model} / {fw}")
        else:
            self.cards["firmware"].set("-")

        self.set_schedule_text()

        status_text = (
            f"{self.com_port_var.get()} | "
            f"{self.data['demand_kw']:.3f} kW | "
            f"{self.pricing_source_var.get()} | "
            f"{self.data['price_cents']:.2f} ¢/kWh | "
            f"${cost_per_hour:.2f}/hr"
        )
        self.status_var.set(status_text)

    def get_active_price_cents(self) -> float:
        source = self.pricing_source_var.get()
        if source == PRICING_SOURCE_EMU:
            return self.data["emu_price_cents"]
        if source == PRICING_SOURCE_COMED:
            return self.data["comed_price_cents"]
        return 0.0

    def on_pricing_source_changed(self, _event=None):
        self.config_data["preferred_pricing_source"] = self.pricing_source_var.get()
        save_app_config(self.config_data)
        if self.pricing_source_var.get() == PRICING_SOURCE_COMED:
            self.fetch_comed_price()
        self.refresh_ui()

    def schedule_comed_refresh(self):
        if self.pricing_source_var.get() == PRICING_SOURCE_COMED:
            self.fetch_comed_price()
        self.root.after(COMED_REFRESH_MS, self.schedule_comed_refresh)

    def fetch_comed_price(self):
        if self.comed_fetch_in_progress:
            return

        self.comed_fetch_in_progress = True
        LOGGER.info("Starting ComEd price refresh")

        def worker():
            try:
                req = urllib_request.Request(
                    COMED_FEED_URL,
                    headers={"User-Agent": "EMU-2-Smart-Meter-Data-Dashboard"}
                )
                with urllib_request.urlopen(req, timeout=10) as response:
                    payload = json.loads(response.read().decode("utf-8"))

                if not payload:
                    raise ValueError("ComEd feed returned no data")

                valid_rows = []
                for item in payload:
                    try:
                        millis_utc = int(item.get("millisUTC", 0))
                        price_cents = float(item["price"])
                    except (KeyError, TypeError, ValueError):
                        continue

                    if not math.isfinite(price_cents):
                        continue

                    valid_rows.append((millis_utc, price_cents))

                if not valid_rows:
                    raise ValueError("ComEd feed returned no valid price rows")

                millis_utc, price_cents = max(valid_rows, key=lambda item: item[0])
                price_time = time.strftime(
                    "%Y-%m-%d %H:%M:%S",
                    time.localtime(millis_utc / 1000.0)
                )
                age_minutes = (time.time() * 1000.0 - millis_utc) / 60000.0
                if age_minutes > 20:
                    LOGGER.warning(
                        "ComEd feed is stale by %.1f minutes; keeping latest available price %.4f c/kWh",
                        age_minutes,
                        price_cents,
                    )
                self.queue.put(("comed_price", (price_cents, price_time)))
            except (ValueError, KeyError, TypeError, urllib_error.URLError) as exc:
                LOGGER.exception("ComEd price fetch failed")
                self.queue.put(("comed_error", f"ComEd price fetch failed: {exc}"))
            except Exception as exc:
                LOGGER.exception("Unexpected ComEd price error")
                self.queue.put(("comed_error", f"Unexpected ComEd price error: {exc}"))

        threading.Thread(target=worker, daemon=True).start()

    def on_close(self):
        self.stop_serial_worker()
        if self.web_server:
            self.web_server.stop()
        try:
            if self.history_conn:
                self.history_conn.close()
        except Exception:
            pass
        self.root.destroy()


class SimpleVar:
    def __init__(self, value=""):
        self.value = value

    def get(self):
        return self.value

    def set(self, value):
        self.value = value


class HeadlessDashboardApp:
    def __init__(self, web_port=8000):
        self.web_port = web_port
        self.queue = queue.Queue()
        self.worker = None
        self.stop_event = threading.Event()
        self.config_data = load_app_config()
        preferred_port = self.config_data.get("preferred_com_port") or COM_PORT
        self.com_port_var = SimpleVar(preferred_port)
        self.available_ports = []
        self.history_lock = threading.Lock()
        self.history_conn = self.init_history_store()
        self.last_history_signature = None
        self.last_history_save_time = ""
        self.status_var = SimpleVar("Starting...")
        self.web_server = DashboardWebServer(self, self.web_port)

        self.data = {
            "device_mac": "",
            "meter_mac": METER_MAC_ID,
            "utc_time": "",
            "local_time": "",
            "demand_kw": 0.0,
            "demand_known": False,
            "price_cents": 0.0,
            "emu_price_cents": 0.0,
            "emu_price_known": False,
            "comed_price_cents": 0.0,
            "comed_price_known": False,
            "current_period_kwh": 0.0,
            "current_period_known": False,
            "lifetime_kwh": 0.0,
            "summation_received_kwh": 0.0,
            "link_strength": "",
            "network_status": "",
            "fw_version": "",
            "model_id": "",
            "last_update": "",
            "price_start": "",
            "price_duration_min": "",
            "comed_price_time": "",
            "schedule_lines": [],
        }

        preferred_pricing_source = self.config_data.get("preferred_pricing_source") or PRICING_SOURCE_NONE
        if preferred_pricing_source not in [PRICING_SOURCE_NONE, PRICING_SOURCE_EMU, PRICING_SOURCE_COMED]:
            preferred_pricing_source = PRICING_SOURCE_NONE
        self.pricing_source_var = SimpleVar(preferred_pricing_source)
        self.comed_fetch_in_progress = False

        self._start_serial()
        self.refresh_ui()
        if self.pricing_source_var.get() == PRICING_SOURCE_COMED:
            self.fetch_comed_price()
        self.web_server.start()

        self.process_thread = threading.Thread(target=self.process_queue_loop, daemon=True)
        self.process_thread.start()
        self.comed_thread = threading.Thread(target=self.comed_refresh_loop, daemon=True)
        self.comed_thread.start()

    def _delayed_call(self, delay_sec, func):
        def runner():
            time.sleep(delay_sec)
            if not self.stop_event.is_set():
                func()
        threading.Thread(target=runner, daemon=True).start()

    def _start_serial(self):
        self.stop_serial_worker()
        self.worker = EmuSerialWorker(self.com_port_var.get(), BAUD_RATE, METER_MAC_ID, self.queue)
        self.worker.start()
        self.status_var.set(f"Waiting for {self.com_port_var.get()}...")

    def stop_serial_worker(self):
        if self.worker:
            self.worker.stop()
            self.worker = None

    def schedule_initial_queries(self):
        self._delayed_call(1.2, self.send_network_info)
        self._delayed_call(1.8, lambda: self.send_named_command("get_device_info", include_meter=False))
        self._delayed_call(2.4, lambda: self.send_named_command("get_time"))
        self._delayed_call(3.0, lambda: self.send_named_command("get_current_price", refresh="Y"))
        self._delayed_call(3.6, lambda: self.send_named_command("get_instantaneous_demand", refresh="Y"))
        self._delayed_call(4.2, lambda: self.send_named_command("get_current_summation_delivered", refresh="Y"))
        self._delayed_call(4.8, lambda: self.send_named_command("get_current_period_usage"))
        self._delayed_call(5.4, self.send_schedule)

    def send_named_command(self, name, include_meter=True, refresh=None, extra_tags=None):
        if self.worker:
            self.worker.send_command(
                name=name,
                include_meter=include_meter,
                refresh=refresh,
                extra_tags=extra_tags
            )

    def send_network_info(self):
        if self.worker:
            self.worker.send_xml(
                "<Command>\n"
                "  <Name>get_network_info</Name>\n"
                "</Command>"
            )

    def send_schedule(self):
        if self.worker:
            self.worker.send_xml(
                "<Command>\n"
                "  <Name>get_schedule</Name>\n"
                "</Command>"
            )

    def append_raw(self, text: str):
        print(text)

    def refresh_history_chart(self):
        return

    def set_schedule_text(self):
        return

    def refresh_ui(self):
        demand_kw = self.data["demand_kw"]
        price_cents = self.get_active_price_cents()
        self.data["price_cents"] = price_cents
        cost_per_hour = demand_kw * (price_cents / 100.0)
        self.status_var.set(
            f"{self.com_port_var.get()} | "
            f"{self.data['demand_kw']:.3f} kW | "
            f"{self.pricing_source_var.get()} | "
            f"{self.data['price_cents']:.2f} ¢/kWh | "
            f"${cost_per_hour:.2f}/hr"
        )

    def process_queue_loop(self):
        while not self.stop_event.is_set():
            try:
                msg_type, payload = self.queue.get(timeout=0.1)
            except queue.Empty:
                continue

            if msg_type == "status":
                self.status_var.set(payload)
                self.append_raw(f"[STATUS] {payload}")
            elif msg_type == "connected":
                self.status_var.set(payload)
                self.append_raw(f"[STATUS] {payload}")
                self.schedule_initial_queries()
            elif msg_type == "error":
                self.status_var.set(payload)
                self.append_raw(f"[ERROR] {payload}")
            elif msg_type == "sent":
                self.append_raw(f"[SENT]\n{payload}\n")
            elif msg_type == "raw":
                self.append_raw(payload)
            elif msg_type == "xml":
                self.handle_xml(payload)
                self.refresh_ui()
                self.maybe_record_history()
            elif msg_type == "comed_price":
                self.comed_fetch_in_progress = False
                price_cents, price_time = payload
                self.data["comed_price_cents"] = price_cents
                self.data["comed_price_known"] = True
                self.data["comed_price_time"] = price_time
                self.data["last_update"] = price_time
                LOGGER.info("ComEd price updated to %.4f c/kWh at %s", price_cents, price_time)
                self.refresh_ui()
                self.maybe_record_history()
            elif msg_type == "comed_error":
                self.comed_fetch_in_progress = False
                LOGGER.error("%s", payload)
                self.append_raw(f"[ERROR] {payload}")
                self.refresh_ui()

    def comed_refresh_loop(self):
        while not self.stop_event.is_set():
            time.sleep(COMED_REFRESH_MS / 1000.0)
            if self.stop_event.is_set():
                break
            if self.pricing_source_var.get() == PRICING_SOURCE_COMED:
                self.fetch_comed_price()

    def stop(self):
        self.stop_event.set()
        self.stop_serial_worker()
        if self.web_server:
            self.web_server.stop()
        try:
            if self.history_conn:
                self.history_conn.close()
        except Exception:
            pass


for _shared_method in (
    "init_history_store",
    "maybe_record_history",
    "load_recent_history",
    "get_snapshot",
    "get_history_payload",
    "get_active_price_known",
    "handle_xml",
    "get_active_price_cents",
    "fetch_comed_price",
):
    setattr(HeadlessDashboardApp, _shared_method, getattr(EmuDashboardApp, _shared_method))


# ============================================================
# Entrypoint
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="EMU-2 Smart Meter Dashboard")
    parser.add_argument("--headless", action="store_true", help="Run without the Tk GUI")
    parser.add_argument("--port", type=int, default=8000, help="Web server port (default: 8000)")
    args = parser.parse_args()

    if args.headless:
        app = HeadlessDashboardApp(web_port=args.port)
        print(f"Headless dashboard running on http://127.0.0.1:{args.port}")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            app.stop()
        return

    root = tk.Tk()
    app = EmuDashboardApp(root, web_port=args.port)
    print(f"Web dashboard running on http://127.0.0.1:{args.port}")
    root.mainloop()


if __name__ == "__main__":
    main()
