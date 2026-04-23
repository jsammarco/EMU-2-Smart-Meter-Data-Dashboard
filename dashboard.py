import json
import math
import queue
import re
import threading
import time
import tkinter as tk
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


# ============================================================
# Helpers
# ============================================================
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
    def __init__(self, root):
        self.root = root
        self.root.title(WINDOW_TITLE)
        self.root.geometry(WINDOW_SIZE)
        self.root.configure(bg="#0F172A")

        self.queue = queue.Queue()
        self.worker = None
        self.config_data = load_app_config()
        preferred_port = self.config_data.get("preferred_com_port") or COM_PORT
        self.com_port_var = tk.StringVar(value=preferred_port)
        self.available_ports = []

        self.data = {
            "device_mac": "",
            "meter_mac": METER_MAC_ID,
            "utc_time": "",
            "local_time": "",
            "demand_kw": 0.0,
            "price_cents": 0.0,
            "emu_price_cents": 0.0,
            "comed_price_cents": 0.0,
            "current_period_kwh": 0.0,
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
        if self.pricing_source_var.get() == PRICING_SOURCE_COMED:
            self.fetch_comed_price()

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
        style.configure("Dashboard.TCombobox", fieldbackground="#0B1220", background="#0B1220", foreground="#F9FAFB")

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

                elif msg_type == "comed_price":
                    self.comed_fetch_in_progress = False
                    price_cents, price_time = payload
                    self.data["comed_price_cents"] = price_cents
                    self.data["comed_price_time"] = price_time
                    self.data["last_update"] = price_time
                    self.refresh_ui()

                elif msg_type == "comed_error":
                    self.comed_fetch_in_progress = False
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

        elif tag == "PriceCluster":
            self.data["device_mac"] = txt("DeviceMacId")
            self.data["meter_mac"] = txt("MeterMacId") or self.data["meter_mac"]
            self.data["emu_price_cents"] = cents_from_pricecluster(
                txt("Price"),
                txt("TrailingDigits")
            )
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

                latest = max(payload, key=lambda item: int(item.get("millisUTC", 0)))
                price_cents = float(latest["price"])
                millis_utc = int(latest["millisUTC"])
                price_time = time.strftime(
                    "%Y-%m-%d %H:%M:%S",
                    time.localtime(millis_utc / 1000.0)
                )
                self.queue.put(("comed_price", (price_cents, price_time)))
            except (ValueError, KeyError, TypeError, urllib_error.URLError) as exc:
                self.queue.put(("comed_error", f"ComEd price fetch failed: {exc}"))
            except Exception as exc:
                self.queue.put(("comed_error", f"Unexpected ComEd price error: {exc}"))

        threading.Thread(target=worker, daemon=True).start()

    def on_close(self):
        self.stop_serial_worker()
        self.root.destroy()


# ============================================================
# Entrypoint
# ============================================================
def main():
    root = tk.Tk()
    app = EmuDashboardApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
