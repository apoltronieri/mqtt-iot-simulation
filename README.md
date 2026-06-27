#  MQTT IoT Simulation — Monitoramento Industrial

Simulação do protocolo **MQTT** aplicado a um cenário de monitoramento industrial em tempo real.  
Três sensores publicam leituras para um broker público na internet, e um servidor de monitoramento coleta e analisa os dados.

---

##  O que é MQTT?

**MQTT** (Message Queuing Telemetry Transport) é um protocolo de comunicação leve criado em 1999 para monitoramento de oleodutos via satélite. Hoje é o padrão da indústria para IoT.

Funciona no modelo **publish/subscribe**:

```
[Sensor] ──► publica num tópico ──► [Broker] ──► entrega pra quem assinou ──► [Servidor]
```

Ninguém fala diretamente com ninguém — o broker fica no meio gerenciando tudo.

---

##  Estrutura do Projeto

```
mqtt-iot-simulation/
├── README.md        ← você está aqui
├── mqtt_iot.py      ← script principal da simulação
└── outputs/         ← gráfico gerado após a execução
```

---

##  Como instalar

Você precisa de **Python 3.8+** e as seguintes bibliotecas:

```bash
pip install paho-mqtt matplotlib psutil
```

| Biblioteca | Para que serve |
|---|---|
| `paho-mqtt` | Biblioteca oficial MQTT para Python |
| `matplotlib` | Geração dos gráficos de métricas |
| `psutil` | Coleta de CPU e memória do processo |

---

## ▶ Como executar

```bash
python mqtt_iot.py
```

A simulação dura **50 segundos** (5 fases × 10 segundos cada).  
O gráfico é salvo automaticamente em `outputs/mqtt_iot_metrics.png` ao final.

> ⚠️ Você precisa de conexão com a internet — a simulação conecta no broker público `broker.hivemq.com`.

---

## ️ Topologia

```
[Sensor Temperatura] ──►
[Sensor Pressão]     ──► [ broker.hivemq.com ] ──► [Servidor de Monitoramento]
[Sensor Vibração]    ──►
```

### Tópicos MQTT

| Tópico | Tipo de dado | QoS |
|---|---|---|
| `fabrica/<sessao>/sensor/temperatura` | Leitura em °C | 0 |
| `fabrica/<sessao>/sensor/pressao` | Leitura em bar | 0 |
| `fabrica/<sessao>/sensor/vibracao` | Leitura em Hz | 0 |
| `fabrica/<sessao>/alarme/critico` | Valor fora do limite | 1 |

**Por que QoS diferente?**  
Leituras normais usam **QoS 0** (sem confirmação) — o sensor publica a cada poucos segundos, perder uma leitura é aceitável.  
Alarmes usam **QoS 1** (com confirmação/PUBACK) — um valor crítico não pode ser perdido.

### Limites dos sensores

| Sensor | Faixa normal | Valor crítico |
|---|---|---|
| Temperatura | 60 – 90 °C | ≥ 100 °C |
| Pressão | 2.0 – 4.0 bar | ≥ 5.0 bar |
| Vibração | 10 – 40 Hz | ≥ 60 Hz |

---

##  Fases de Carga

A simulação passa por 5 fases de 10 segundos cada, com volumes de mensagem crescentes:

| Fase | Nome | Comportamento |
|---|---|---|
| 1 | Operação Normal | Leituras estáveis dentro dos limites |
| 2 | Aumento de Carga | Máquinas aceleradas, mais leituras |
| 3 | Pico de Produção | Todos os sensores no máximo |
| 4 | Falha Iminente | Valores anômalos + alarmes críticos |
| 5 | Normalização | Valores voltando ao normal |

---

## Métricas coletadas

| Gráfico | O que mede | O que esperar ver |
|---|---|---|
| **Latência** | Tempo entre publish e receive (ms) | Sobe na Fase 3 e Fase 4 |
| **Throughput** | Mensagens recebidas por segundo | Pico na Fase 3 |
| **Alarmes críticos** | Contagem acumulada de alarmes QoS 1 | Degraus na Fase 4 |
| **Memória RAM** | RAM consumida pelo processo Python (MB) | Cresce levemente com filas cheias |
| **CPU** | % de CPU do processo | Pico na Fase 3 |
| **Banda acumulada** | Total de bytes recebidos | Curva sempre crescente |

As **linhas vermelhas pontilhadas** nos gráficos marcam o momento exato de cada alarme crítico.

---

##  Estrutura do código

```
mqtt_iot.py
├── MetricsCollector   — armazena todas as métricas (thread-safe)
├── MonitoringServer   — subscriber MQTT (servidor de monitoramento)
├── IndustrialSensor   — publisher MQTT (cada sensor é uma thread)
├── SystemSampler      — coleta CPU e memória a cada 100ms
├── plot_results()     — gera os 6 gráficos com matplotlib
└── run_simulation()   — orquestra fases e inicia todas as threads
```

Cada sensor roda como uma **thread independente** — assim os três publicam em paralelo, como acontece na realidade.

---

## 📚 Referências

- [Especificação MQTT 3.1.1 — OASIS](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html)
- [Documentação paho-mqtt](https://eclipse.dev/paho/files/paho.mqtt.python/html/index.html)
- [HiveMQ — Broker público de testes](https://www.hivemq.com/public-mqtt-broker/)

