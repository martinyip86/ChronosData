# 🚀 Hydra-Feed:Resilient Crypto Market Data Infrastructure

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker Support](https://img.shields.io/badge/Docker-Supported-blue)](https://www.docker.com/)

**Hydra-Feed** is a high-performance, highly reliable pipeline for accessing real-time cryptocurrency market data.
Designed specifically for quantitative trading, it resolves the critical issue of data gaps caused by WebSocket disconnections, ensuring **100% integrity** of backtesting data.

## 🏛 Architecture

The project adopts a **Producer-Consumer** architecture, achieving full-link decoupling across data collection,validation,storage:

1. **Collectors (Asyncio):** Millisecond-level access to Binance and OKX L2 Order and Trades.
2. **Redis Buffer:** As high-speed middleware, it smooths out I/O pressure during periods of severe market volatility.
3. **GapFiller:** Continuously monitor the 'trade_id' sequence and automatically trigger the REST API to backfill missing data.
4. **DB Syncer:** Adaptive batch writing for Clickhouse, enabling sub-second retrieval of hundreds of millions of tick data records.
5. **Monitoring:** Fully integrate Prometheus & Grafana to monitor the system's pulse in real time.

## ✨ Core Features

* **Ultra-Fast Asynchronous architecture:** Based on `Python Asyncio` and `ccxt.pro`, a single instance supports the concurrent collection of data from multiple exchanges and trading pairs.
* **Data Self-Healing System:** The proprietary `GapFiller` logic automatically dispatches gap-filling tasks upon detecting data gaps,achieving true "zero packet loss" storage.
* **Industrial-Grade Operations and Maintenance:** A complete `docker-compose` deployment solution, featuring strict resource limits(Memory Limits) and health checks.
* **Production-Grade Monitoring:** Provides Grafana dashboards for real-time monitoring of throughput and link latency.

---

## 🛠 Quick Start

### 1. Clone Project
```bash
git clone [https://github.com/martinyip86/ChronosData.git]
cd ChronosData
```

### 2. Environment Setup
Create a `.env` file and fill in your API information (refer to `.env.example`):

BINANCE_API_KEY=your_key
BINANCE_SECRET=your_secret
CLICKHOUSE_HOST=your_host
CLICKHOUSE_PORT=your_port
CLICKHOUSE_USER=your_user
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DB=your_DB

### 3. One-touch Start
docker-compose up -d

Once started,you can access the monitoring interface at the following address:

Grafana: http://localhost:3000 (default password: administration)

Prometheus: http://localhost:9090

📂 Project Structure
.
├── src/
│   ├── collectors/      # Exchange WebSocket Integration Logic (StreamCommander)
│   ├── workers/         # Syncer and GapFiller
│   ├── storage/         # Redis & ClickHouse Client-side Encapsulation
│   ├── models/          # Data Model and SQL Schema
│   └── utils/           # log and util function
├── infra/               # Prometheus & Grafana config data
├── docker-compose.yml   # Industrial-Grade Container Orchestration
└── .env.example         # Environment Variable Template

🎯 Vision

This project is dedicated to building the most robust underlying data infrastructure to support cutting-edge strategy research,striving toward the pinnacle of the quantitative data domain.