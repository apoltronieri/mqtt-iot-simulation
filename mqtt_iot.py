"""
Simulação MQTT — Monitoramento Industrial IoT
==============================================
Cenário: sensores de uma fábrica publicam leituras em tempo real.
Broker público: broker.hivemq.com:1883

Topologia:
  [Sensor Temperatura] ──►
  [Sensor Pressão]     ──► [broker.hivemq.com] ──► [Servidor de Monitoramento]
  [Sensor Vibração]    ──►

Tópicos:
  fabrica/<sessao>/sensor/temperatura   — leitura em °C  (QoS 0)
  fabrica/<sessao>/sensor/pressao       — leitura em bar (QoS 0)
  fabrica/<sessao>/sensor/vibracao      — leitura em Hz  (QoS 0)
  fabrica/<sessao>/alarme/critico       — valor fora do limite (QoS 1)

QoS 0 para leituras normais: sensores publicam a cada poucos segundos,
perder uma leitura é aceitável. QoS 1 para alarmes: não podem ser perdidos.

Fases (10s cada):
  Fase 1 — Operação Normal   : leituras estáveis dentro do limite
  Fase 2 — Aumento de Carga  : máquinas aceleradas, mais leituras
  Fase 3 — Pico de Produção  : spike, todos sensores no máximo
  Fase 4 — Falha Iminente    : leituras anômalas + alarmes críticos
  Fase 5 — Normalização      : valores voltando ao normal
"""

import threading
import time
import random
import uuid
import json
import psutil
import os
import sys

import paho.mqtt.client as mqtt
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# ─────────────────────────────────────────────
# CONFIGURAÇÕES
# ─────────────────────────────────────────────

BROKER_HOST    = "broker.hivemq.com"
BROKER_PORT    = 1883
PHASE_DURATION = 10
KEEP_ALIVE     = 60

# Prefixo único por execução — evita colisão no broker público
SESSION_ID = str(uuid.uuid4())[:8]
BASE_TOPIC = f"fabrica/{SESSION_ID}"

# Mensagens por fase por sensor
PHASE_LOAD = {
    1: 3,
    2: 6,
    3: 14,
    4: 8,
    5: 4,
}

PHASES = {
    1: "Operação Normal",
    2: "Aumento de Carga",
    3: "Pico de Produção",
    4: "Falha Iminente",
    5: "Normalização",
}

# Limites normais de operação de cada sensor
SENSOR_LIMITS = {
    "temperatura": {"min": 60,  "max": 90,  "unit": "°C",  "critical": 100},
    "pressao":     {"min": 2.0, "max": 4.0, "unit": "bar", "critical": 5.0},
    "vibracao":    {"min": 10,  "max": 40,  "unit": "Hz",  "critical": 60},
}

# ─────────────────────────────────────────────
# COLETOR DE MÉTRICAS
# ─────────────────────────────────────────────

class MetricsCollector:
    """
    Armazena séries temporais de cada métrica.
    Todas as operações são thread-safe via Lock.
    """
    def __init__(self):
        self._lock             = threading.Lock()
        self.latencies         = []   # (t, ms)    — publish → receive
        self.throughputs       = []   # (t, msg/s) — msgs recebidas por segundo
        self.retransmits       = []   # (t, count) — desconexões acumuladas
        self.memory_mb         = []   # (t, MB)    — RAM do processo
        self.cpu_pct           = []   # (t, %)     — CPU do processo
        self.bytes_total       = []   # (t, bytes) — banda acumulada
        self.phase_markers     = []   # (t, label)
        self.alarm_markers     = []   # (t,)       — momentos de alarme

        self._msg_timestamps   = []
        self._retransmit_count = 0
        self._bytes_acc        = 0
        self._proc             = psutil.Process(os.getpid())

    def record_delivery(self, sent_at, payload_size):
        now = time.time()
        with self._lock:
            self.latencies.append((now, (now - sent_at) * 1000))
            self._msg_timestamps.append(now)
            self._bytes_acc += payload_size

    def record_alarm(self):
        with self._lock:
            self.alarm_markers.append(time.time())

    def record_retransmit(self):
        with self._lock:
            self._retransmit_count += 1

    def record_phase(self, phase_num, label):
        with self._lock:
            self.phase_markers.append((time.time(), f"Fase {phase_num}: {label}"))

    def sample_system(self):
        now    = time.time()
        window = 2.0
        with self._lock:
            self._msg_timestamps = [t for t in self._msg_timestamps if now - t <= window]
            tput = len(self._msg_timestamps) / window
            mem  = self._proc.memory_info().rss / (1024 * 1024)
            cpu  = self._proc.cpu_percent(interval=None)

            self.throughputs.append((now, tput))
            self.memory_mb.append((now, mem))
            self.cpu_pct.append((now, cpu))
            self.bytes_total.append((now, self._bytes_acc))
            self.retransmits.append((now, self._retransmit_count))


metrics = MetricsCollector()

# ─────────────────────────────────────────────
# SUBSCRIBER — Servidor de Monitoramento
# ─────────────────────────────────────────────

class MonitoringServer:
    """
    Subscriber MQTT.
    Assina todos os tópicos da sessão e coleta métricas de entrega.
    Detecta alarmes críticos e os registra separadamente.
    """
    def __init__(self):
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"monitor_{SESSION_ID}"
        )
        self.client.on_connect    = self._on_connect
        self.client.on_message    = self._on_message
        self.client.on_disconnect = self._on_disconnect
        self.connected = threading.Event()

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            client.subscribe(f"{BASE_TOPIC}/#", qos=1)
            self.connected.set()
            print(f"  [Monitor] Conectado — assinando {BASE_TOPIC}/#")
        else:
            print(f"  [Monitor] Falha na conexão: {reason_code}")

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            metrics.record_delivery(payload["sent_at"], len(msg.payload))
            if "alarme" in msg.topic:
                metrics.record_alarm()
                sensor  = payload.get("sensor", "?")
                valor   = payload.get("valor", "?")
                unit    = payload.get("unit", "")
                print(f"  [ALARME] {sensor} = {valor}{unit}")
        except Exception:
            pass

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        if reason_code != 0:
            metrics.record_retransmit()

    def start(self):
        self.client.connect(BROKER_HOST, BROKER_PORT, KEEP_ALIVE)
        self.client.loop_start()
        self.connected.wait(timeout=10)

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()


# ─────────────────────────────────────────────
# PUBLISHER — Sensor Industrial
# ─────────────────────────────────────────────

class IndustrialSensor(threading.Thread):
    """
    Publisher MQTT — simula um sensor industrial.
    Publica leituras com QoS 0 (normal) e QoS 1 (alarme crítico).
    O comportamento muda por fase:
      - Normal/Carga: valores dentro dos limites
      - Pico: valores no topo do limite
      - Falha: valores anômalos, probabilidade alta de alarme
      - Normalização: valores caindo de volta
    """
    def __init__(self, sensor_type, phase_signal, stop_event, anomaly_event):
        super().__init__(daemon=True, name=f"Sensor-{sensor_type}")
        self.sensor_type  = sensor_type
        self.limits       = SENSOR_LIMITS[sensor_type]
        self.phase_signal = phase_signal
        self.stop_event   = stop_event
        self.anomaly_event = anomaly_event

        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"sensor_{sensor_type}_{SESSION_ID}"
        )
        self.client.on_connect = lambda c, u, f, rc, p: self._connected.set() if rc == 0 else None
        self._connected = threading.Event()

    def _read_value(self, phase):
        """
        Gera leitura simulada do sensor de acordo com a fase.
        Fase 1-2: normal. Fase 3: topo. Fase 4: anômalo. Fase 5: descendo.
        """
        lo = self.limits["min"]
        hi = self.limits["max"]
        cr = self.limits["critical"]

        if phase == 1:
            return round(random.uniform(lo, lo + (hi - lo) * 0.5), 2)
        elif phase == 2:
            return round(random.uniform(lo, hi), 2)
        elif phase == 3:
            return round(random.uniform(hi * 0.85, hi), 2)
        elif phase == 4:
            # 40% chance de valor anômalo acima do crítico
            if random.random() < 0.4:
                return round(random.uniform(hi, cr * 1.1), 2)
            return round(random.uniform(hi * 0.9, cr), 2)
        else:  # fase 5
            return round(random.uniform(lo, lo + (hi - lo) * 0.6), 2)

    def _publish_reading(self, phase):
        valor  = self._read_value(phase)
        topic  = f"{BASE_TOPIC}/sensor/{self.sensor_type}"
        payload = json.dumps({
            "sensor":  self.sensor_type,
            "valor":   valor,
            "unit":    self.limits["unit"],
            "sent_at": time.time(),
        }).encode()

        self.client.publish(topic, payload, qos=0)

        # Publica alarme se valor ultrapassa o crítico
        if valor >= self.limits["critical"]:
            alarm_payload = json.dumps({
                "sensor":  self.sensor_type,
                "valor":   valor,
                "unit":    self.limits["unit"],
                "limite":  self.limits["critical"],
                "sent_at": time.time(),
            }).encode()
            self.client.publish(f"{BASE_TOPIC}/alarme/critico", alarm_payload, qos=1)

        return len(payload)

    def run(self):
        self.client.connect(BROKER_HOST, BROKER_PORT, KEEP_ALIVE)
        self.client.loop_start()
        self._connected.wait(timeout=10)

        last_phase = None
        while not self.stop_event.is_set():
            phase = self.phase_signal["current"]

            if phase != last_phase:
                last_phase = phase
                n_msgs = PHASE_LOAD.get(phase, 3)

                for _ in range(n_msgs):
                    if self.stop_event.is_set():
                        break
                    self._publish_reading(phase)
                    time.sleep(random.uniform(0.1, 0.4))

            time.sleep(0.1)

        self.client.loop_stop()
        self.client.disconnect()


# ─────────────────────────────────────────────
# SAMPLER DE SISTEMA
# ─────────────────────────────────────────────

class SystemSampler(threading.Thread):
    def __init__(self, stop_event):
        super().__init__(daemon=True)
        self.stop_event = stop_event

    def run(self):
        while not self.stop_event.is_set():
            metrics.sample_system()
            time.sleep(0.1)


# ─────────────────────────────────────────────
# GERAÇÃO DE GRÁFICOS
# ─────────────────────────────────────────────

PHASE_COLORS = ["#EAF2FB", "#FEF9E7", "#FDEDEC", "#F9EBEA", "#EAFAF1"]
COLORS = {
    "latency":    "#4C72B0",
    "throughput": "#DD8452",
    "retransmit": "#C44E52",
    "memory":     "#55A868",
    "cpu":        "#8172B2",
    "bytes":      "#937860",
}

def _add_phase_bands(ax, phase_markers, t0):
    times = [t - t0 for t, _ in phase_markers]
    times.append(times[-1] + PHASE_DURATION)
    for i in range(len(times) - 1):
        ax.axvspan(times[i], times[i+1],
                   alpha=0.25, color=PHASE_COLORS[i % len(PHASE_COLORS)], zorder=0)
        ax.axvline(times[i], color="#AAAAAA", linewidth=0.8, linestyle="--", alpha=0.6)

def _add_alarm_lines(ax, alarm_markers, t0):
    """Marca verticalmente cada momento em que um alarme crítico foi disparado."""
    for t in alarm_markers:
        ax.axvline(t - t0, color="#E74C3C", linewidth=1.0,
                   linestyle=":", alpha=0.7, zorder=5)

def _rel(series, t0):
    return [(t - t0, v) for t, v in series]

def plot_results(m: MetricsCollector):
    if not m.phase_markers:
        print("Sem dados para plotar.")
        return

    t0  = m.phase_markers[0][0]
    fig, axes = plt.subplots(3, 2, figsize=(14, 10))
    fig.suptitle(
        "MQTT — Monitoramento Industrial IoT\nMétricas por Fase de Carga",
        fontsize=13, fontweight="bold"
    )
    fig.subplots_adjust(hspace=0.5, wspace=0.3)
    axs = axes.flatten()

    # ── 1. Latência ────────────────────────────
    ax = axs[0]
    data = _rel(m.latencies, t0)
    if data:
        xs, ys = zip(*data)
        ax.scatter(xs, ys, s=10, alpha=0.5, color=COLORS["latency"], zorder=3, label="Por msg")
        if len(ys) >= 5:
            w  = 5
            ma = [sum(ys[max(0,i-w):i+1]) / min(i+1,w) for i in range(len(ys))]
            ax.plot(xs, ma, color=COLORS["latency"], linewidth=2, label="Média móvel", zorder=4)
        ax.legend(fontsize=7)
    _add_phase_bands(ax, m.phase_markers, t0)
    _add_alarm_lines(ax, m.alarm_markers, t0)
    ax.set_title("Latência (publish → receive)", fontsize=10)
    ax.set_ylabel("ms", fontsize=9)
    ax.set_xlabel("Tempo (s)", fontsize=9)
    ax.grid(True, alpha=0.3)

    # ── 2. Throughput ──────────────────────────
    ax = axs[1]
    data = _rel(m.throughputs, t0)
    if data:
        xs, ys = zip(*data)
        ax.plot(xs, ys, color=COLORS["throughput"], linewidth=1.5)
        ax.fill_between(xs, ys, alpha=0.15, color=COLORS["throughput"])
    _add_phase_bands(ax, m.phase_markers, t0)
    _add_alarm_lines(ax, m.alarm_markers, t0)
    ax.set_title("Throughput", fontsize=10)
    ax.set_ylabel("msgs / s", fontsize=9)
    ax.set_xlabel("Tempo (s)", fontsize=9)
    ax.grid(True, alpha=0.3)

    # ── 3. Alarmes críticos acumulados ─────────
    ax = axs[2]
    if m.alarm_markers:
        alarm_times = sorted([t - t0 for t in m.alarm_markers])
        alarm_counts = list(range(1, len(alarm_times) + 1))
        ax.step(alarm_times, alarm_counts, color=COLORS["retransmit"],
                linewidth=1.5, where="post")
        ax.fill_between(alarm_times, alarm_counts, step="post",
                        alpha=0.15, color=COLORS["retransmit"])
    _add_phase_bands(ax, m.phase_markers, t0)
    ax.set_title("Alarmes críticos acumulados (QoS 1)", fontsize=10)
    ax.set_ylabel("contagem", fontsize=9)
    ax.set_xlabel("Tempo (s)", fontsize=9)
    ax.grid(True, alpha=0.3)

    # ── 4. Memória ─────────────────────────────
    ax = axs[3]
    data = _rel(m.memory_mb, t0)
    if data:
        xs, ys = zip(*data)
        ax.plot(xs, ys, color=COLORS["memory"], linewidth=1.5)
        ax.fill_between(xs, ys, alpha=0.15, color=COLORS["memory"])
    _add_phase_bands(ax, m.phase_markers, t0)
    ax.set_title("Memória RAM do processo", fontsize=10)
    ax.set_ylabel("MB", fontsize=9)
    ax.set_xlabel("Tempo (s)", fontsize=9)
    ax.grid(True, alpha=0.3)

    # ── 5. CPU ─────────────────────────────────
    ax = axs[4]
    data = _rel(m.cpu_pct, t0)
    if data:
        xs, ys = zip(*data)
        ax.plot(xs, ys, color=COLORS["cpu"], linewidth=1.5)
        ax.fill_between(xs, ys, alpha=0.15, color=COLORS["cpu"])
    _add_phase_bands(ax, m.phase_markers, t0)
    ax.set_title("CPU do processo", fontsize=10)
    ax.set_ylabel("%", fontsize=9)
    ax.set_xlabel("Tempo (s)", fontsize=9)
    ax.set_ylim(bottom=0)
    ax.grid(True, alpha=0.3)

    # ── 6. Bytes trafegados ────────────────────
    ax = axs[5]
    data = _rel(m.bytes_total, t0)
    if data:
        xs, ys = zip(*data)
        ax.plot(xs, ys, color=COLORS["bytes"], linewidth=1.5)
        ax.fill_between(xs, ys, alpha=0.15, color=COLORS["bytes"])
    _add_phase_bands(ax, m.phase_markers, t0)
    ax.set_title("Banda acumulada (bytes recebidos)", fontsize=10)
    ax.set_ylabel("bytes", fontsize=9)
    ax.set_xlabel("Tempo (s)", fontsize=9)
    ax.grid(True, alpha=0.3)

    # Legenda de fases + alarmes
    patches = [
        mpatches.Patch(color=PHASE_COLORS[i], alpha=0.5,
                       label=f"Fase {i+1}: {list(PHASES.values())[i]}")
        for i in range(5)
    ]
    patches.append(mpatches.Patch(color="#E74C3C", alpha=0.5, label="Alarme crítico"))
    fig.legend(handles=patches, loc="lower center", ncol=6,
               fontsize=8, framealpha=0.8, bbox_to_anchor=(0.5, -0.02))

    out = "outputs/mqtt_iot_metrics.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    print(f"\nGráfico salvo: {out}")
    return out


# ─────────────────────────────────────────────
# ORQUESTRADOR
# ─────────────────────────────────────────────

def run_simulation():
    print("=" * 55)
    print("  MQTT Simulation — Monitoramento Industrial IoT")
    print(f"  Broker : {BROKER_HOST}:{BROKER_PORT}")
    print(f"  Tópico : {BASE_TOPIC}")
    print("=" * 55)

    stop_event    = threading.Event()
    anomaly_event = threading.Event()
    phase_signal  = {"current": 1}

    monitor = MonitoringServer()
    try:
        monitor.start()
    except Exception as e:
        print(f"\nErro ao conectar no broker: {e}")
        print("Verifique sua conexão com a internet.")
        sys.exit(1)

    sensors = [
        IndustrialSensor(s, phase_signal, stop_event, anomaly_event)
        for s in ["temperatura", "pressao", "vibracao"]
    ]
    for s in sensors:
        s.start()

    sampler = SystemSampler(stop_event)
    sampler.start()

    for phase_num, phase_label in PHASES.items():
        print(f"\n[t={( phase_num-1)*PHASE_DURATION:02d}s] Fase {phase_num}: {phase_label}")
        metrics.record_phase(phase_num, phase_label)
        phase_signal["current"] = phase_num

        if phase_num == 4:
            print("  ⚠ Anomalias ativadas — valores críticos possíveis")
            anomaly_event.set()
        else:
            anomaly_event.clear()

        time.sleep(PHASE_DURATION)

    print("\n[Simulação encerrada] Aguardando últimas mensagens...")
    stop_event.set()
    time.sleep(2)
    monitor.stop()

    return metrics


# ─────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    m = run_simulation()
    plot_results(m)
    print("Pronto!")

