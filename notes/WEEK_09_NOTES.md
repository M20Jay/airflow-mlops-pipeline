# Week 9 — Apache Airflow ML Pipeline Orchestration
**Martin James Ng'ang'a · MLOps Engineer · Nairobi, Kenya 🇰🇪**
`github.com/M20Jay` · Week 9 of 15 · Built: June 2026

---

## Overview

Apache Airflow deployed as the orchestration layer for the air quality ML pipeline built in Week 6. Two production DAGs automate what was previously done manually — data ingestion, model retraining, and parallel model comparison — scheduled every Monday at 5am without human intervention.

**The core problem solved:** A model trained once and never monitored is not a production system — it is a time bomb. Airflow ensures the air quality model retrains automatically every week on fresh OpenAQ data, catching seasonal drift before it degrades forecast accuracy.

---

## Final Results

| Component | Details |
|-----------|---------|
| Airflow Version | 2.10.5 |
| Deployment | Docker Compose on AWS EC2 t3.medium Frankfurt |
| Server IP | 52.57.213.229 |
| Executor | LocalExecutor — no worker container |
| Airflow UI | http://52.57.213.229:8080 |
| DAGs | 2 production DAGs |
| Email alerting | Gmail SMTP via App Password |
| Auto-restart | Systemd service enabled |

---

## Architecture

```
AWS EC2 t3.medium Frankfurt (52.57.213.229)
    ↓
Docker Compose — 5 Airflow containers
    ├── airflow-webserver  → UI on port 8080
    ├── airflow-scheduler  → reads DAGs, triggers tasks
    ├── airflow-triggerer  → handles deferred tasks
    ├── postgres           → Airflow metadata database
    └── redis              → task queue broker
    ↓
~/airflow-docker/dags/
    ├── air_quality_dag.py          → weekly retraining pipeline
    └── parallel_training_dag.py   → parallel model comparison
    ↓
Systemd service → auto-starts on server reboot
```

**File locations on server:**

```
~/airflow-docker/                     → Airflow project root
~/airflow-docker/dags/                → DAG files (Airflow watches this folder)
~/airflow-docker/logs/                → Task execution logs
~/airflow-docker/docker-compose.yaml  → Container configuration
~/airflow-docker/.env                 → Environment variables
/etc/systemd/system/airflow.service   → Auto-start service definition
```

---

## DAGs Built

### DAG 1 — air_quality_retrain

**File:** `dags/air_quality_dag.py`
**Schedule:** Every Monday at 5am (`0 5 * * 1`)
**Tasks:** 3 sequential

```
pull_data → retrain_model → evaluate_model
```

| Task | Purpose |
|------|---------|
| `pull_data` | Fetches fresh PM2.5 readings from OpenAQ API |
| `retrain_model` | Retrains ARIMA model on updated dataset |
| `evaluate_model` | Compares new RMSE against baseline 9.93 µg/m³ |

**Failure callback pattern:**

```python
def notify_failure(context):
    send_email(
        to='ngangam93@gmail.com',
        subject=f'Airflow Alert — {context["dag"].dag_id} FAILED',
        html_content=f"<p>Task: {context['task'].task_id} failed</p>"
    )

default_args = {
    'on_failure_callback': notify_failure,
    'email_on_failure': False,
}
```

---

### DAG 2 — parallel_model_training

**File:** `dags/parallel_training_dag.py`
**Schedule:** Every Monday at 5am (`0 5 * * 1`)
**Tasks:** 5 with parallel execution

```
pull_data → [train_lstm + train_arima] → evaluate_models → notify
```

| Task | Runs | Purpose |
|------|------|---------|
| `pull_data` | Sequential | Fetches fresh data |
| `train_lstm` | **Parallel** | Trains LSTM model |
| `train_arima` | **Parallel** | Trains ARIMA model |
| `evaluate_models` | Sequential | Compares RMSE — selects winner |
| `notify` | Sequential | Reports completion |

**Parallel execution syntax:**

```python
ingest >> [lstm, arima] >> evaluate >> alert
# Square brackets = run both simultaneously
```

---

## Key Concepts

### Apache Airflow vs Cron

| Feature | Cron | Apache Airflow |
|---------|------|----------------|
| Task dependencies | None | Explicit — pull before retrain |
| Failure handling | Silent failure | Retry logic + email alerts |
| Visibility | No logs | Full task logs in UI |
| Parallel execution | Not supported | Native `[task1, task2]` |
| Run history | None | Full history with durations |
| Monitoring | None | Live dashboard |

### DAG — Directed Acyclic Graph

```
Directed  → tasks have a defined order
Acyclic   → no circular loops — task cannot trigger itself
Graph     → visualised as connected nodes

Sequential:  pull_data >> retrain >> evaluate
Parallel:    pull_data >> [train_lstm, train_arima] >> evaluate
```

### LocalExecutor vs CeleryExecutor

```
LocalExecutor:
  Runs tasks in same process as scheduler
  No worker container — saves ~400MB RAM
  Correct for single-engineer setups on t3.medium
  Used in this pipeline

CeleryExecutor:
  Adds distributed worker containers via Redis
  Designed for large teams running hundreds of DAGs
  Requires more RAM — caused OOM kills on t3.small
  Switched away from this in Week 9
```

### Cron Schedule Syntax

```
0 5 * * 1
│ │ │ │ └── Day of week (1 = Monday)
│ │ │ └──── Month (* = every month)
│ │ └────── Day of month (* = every day)
│ └──────── Hour (5 = 5am UTC)
└────────── Minute (0 = on the hour)
```

### catchup=False

```
If DAG was paused for 3 weeks:
  catchup=True  → triggers 3 back-to-back runs immediately
  catchup=False → runs only from now forward

Always use catchup=False in production
to prevent accidental backfill runs
```

### on_failure_callback

```
More flexible than Airflow's built-in email_on_failure
Allows:
  Custom HTML email content
  Multiple recipients
  Slack integration
  Custom logging
  Conditional alerts based on task type
```

### Swap Space

```
Emergency RAM using disk space
Added 2GB swap before installing Airflow
to protect 8 production APIs from OOM kills

sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile

Slower than real RAM but prevents server crashes
during memory spikes
```

---

## Email Alerting Setup

### Gmail App Password

```
1. myaccount.google.com → Security → App passwords
2. Create password named "Airflow"
3. 16-character password generated
4. More secure than account password
   scoped to one app only
```

### SMTP Configuration in docker-compose.yaml

```yaml
AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
AIRFLOW__SMTP__SMTP_STARTTLS: "True"
AIRFLOW__SMTP__SMTP_SSL: "False"
AIRFLOW__SMTP__SMTP_PORT: "587"
AIRFLOW__SMTP__SMTP_MAIL_FROM: your@gmail.com
AIRFLOW__SMTP__SMTP_USER: your@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD: your-app-password
```

**Port 587 with STARTTLS** — industry standard for authenticated email with encryption. Not port 465 (SSL) or port 25 (unencrypted).

---

## CLI Reference

### SSH to Server

```bash
ssh -i ~/Documents/GitHub/mlops-key.pem ubuntu@52.57.213.229
# Note: IP may change if EC2 stops — Week 10 fixes with Elastic IP
```

### Copy DAG Files (SCP)

```bash
# Server → Mac (download)
scp -i ~/Documents/GitHub/mlops-key.pem \
  ubuntu@52.57.213.229:~/airflow-docker/dags/air_quality_dag.py \
  ~/Documents/GitHub/airflow-mlops-pipeline/dags/

# Mac → Server (upload)
scp -i ~/Documents/GitHub/mlops-key.pem \
  ~/Documents/GitHub/airflow-mlops-pipeline/dags/new_dag.py \
  ubuntu@52.57.213.229:~/airflow-docker/dags/
```

### Start and Stop Airflow

```bash
# Start without worker (saves RAM)
cd ~/airflow-docker
docker compose up -d --scale airflow-worker=0

# Check all containers healthy
docker compose ps

# Stop Airflow
docker compose down

# Check memory after starting
free -h
```

### View Logs

```bash
# Scheduler logs
docker logs airflow-docker-airflow-scheduler-1 2>&1 | tail -50

# Filter errors only
docker logs airflow-docker-airflow-scheduler-1 2>&1 | grep -i "error\|failed" | tail -20

# Check SMTP config loaded
docker exec airflow-docker-airflow-scheduler-1 env | grep SMTP
```

### DAG Management

```bash
# List all DAGs
docker exec airflow-docker-airflow-scheduler-1 airflow dags list

# Check specific DAG loaded
docker exec airflow-docker-airflow-scheduler-1 \
  airflow dags list 2>&1 | grep air_quality

# Trigger DAG manually
docker exec airflow-docker-airflow-scheduler-1 \
  airflow dags trigger air_quality_retrain

# Pause / unpause
docker exec airflow-docker-airflow-scheduler-1 \
  airflow dags pause parallel_model_training
docker exec airflow-docker-airflow-scheduler-1 \
  airflow dags unpause parallel_model_training
```

### Systemd Service

```bash
sudo systemctl enable airflow.service   # auto-start on reboot
sudo systemctl disable airflow.service  # disable auto-start
sudo systemctl status airflow.service   # check status
sudo systemctl restart airflow.service  # restart
```

### Memory Management

```bash
free -h                                          # check memory
ps aux --sort=-%mem | head -15                   # top memory users
docker ps --format "table {{.Names}}\t{{.Status}}" # all containers
```

---

## Debugging Reference

### Common Errors and Fixes

| Error | Fix |
|-------|-----|
| Airflow crashed overnight | t3.small OOM — switched to LocalExecutor, eliminated worker + Redis |
| Swap hit 100% on restart | Systemd auto-started Airflow before memory stabilised — disabled systemd until t3.medium upgrade |
| Python 3.14 incompatible | Airflow requires Python below 3.13 — used Docker, Airflow container has Python 3.12 internally |
| t3.medium not available | AWS free plan restriction — upgraded to paid plan |
| IP changed after restart | EC2 assigns new public IP on each start — updated all READMEs, permanent fix Week 10 Elastic IP |
| SMTP not loading | .env variables not passed into containers — added SMTP settings directly to docker-compose.yaml |
| DAG not appearing in UI | Import error in DAG file — check scheduler logs for syntax errors |
| OOM kill during UI browsing | Opening Airflow UI triggers memory spike — check free -h before opening |

### Debugging Order

```
1. Check memory first: free -h
   If swap > 80% → stop Airflow immediately
   
2. Check containers: docker compose ps
   All 5 should show healthy or running

3. Check scheduler logs:
   docker logs airflow-docker-airflow-scheduler-1 2>&1 | tail -50

4. Check DAG loaded:
   docker exec airflow-docker-airflow-scheduler-1 airflow dags list

5. Check SMTP config:
   docker exec airflow-docker-airflow-scheduler-1 env | grep SMTP

6. Trigger manually and watch:
   docker exec airflow-docker-airflow-scheduler-1 \
     airflow dags trigger air_quality_retrain
```

---

## Challenges and Solutions

| Challenge | Root Cause | Solution |
|-----------|-----------|----------|
| Airflow crashed overnight | t3.small OOM — 6 containers used all RAM | Switched to LocalExecutor — eliminated worker container |
| Swap hit 100% on restart | Systemd auto-started Airflow before memory stabilised | Disabled systemd until t3.medium upgrade |
| Python 3.14 incompatible | All Airflow versions require Python below 3.13 | Used Docker — Airflow container has Python 3.12 internally |
| t3.medium not available | AWS free plan restriction on instance types | Upgraded AWS account to paid plan |
| IP changed after restart | EC2 assigns new public IP on each start | Updated all READMEs — permanent fix in Week 10 via Elastic IP |
| SMTP not loading in containers | .env variables not passed into containers by default | Added SMTP settings directly to docker-compose.yaml environment section |

---

## Deep Dives — Critical Concepts

### Why Airflow Uses DAGs and Not Simple Queues

A task queue runs jobs in order — first in first out. It has no concept of dependencies or conditions. Airflow uses DAGs because ML pipelines have conditional logic:

```
Data ingestion must complete before preprocessing
Preprocessing must complete before training
Training must complete before evaluation
Evaluation result determines whether to deploy

If any step fails:
  Queue: job lost silently
  Airflow: retries automatically, alerts on failure,
           shows exactly which task failed and why,
           preserves all logs for debugging
```

### The Memory Problem on t3.small — What We Learned

```
t3.small: 2GB RAM

8 API containers:     ~1.2GB
Airflow webserver:    ~300MB
Airflow scheduler:    ~200MB
Airflow postgres:     ~150MB
Airflow redis:        ~100MB
OS overhead:          ~200MB
─────────────────────────────
Total needed:         ~2.15GB
Available:            2.0GB

OOM kill is inevitable

Solution: t3.medium (4GB RAM)
  Same cost: $30/month
  Double the RAM
  All systems run comfortably at ~55% memory
```

### SCP — Secure Copy Protocol

```
scp = copy files between computers over SSH
      same security as SSH — uses your .pem key

Direction matters:
  Server → Mac:  scp -i key.pem user@server:/remote/file ~/local/
  Mac → Server:  scp -i key.pem ~/local/file user@server:/remote/

The colon : separates server address from file path
Everything before : = the server
Everything after :  = the path on that server
```

### Idempotency in DAGs — Why catchup=False

```
A DAG must be idempotent — running it twice
should give the same result as running it once

Risks without idempotency:
  DAG runs → pulls data → saves to database
  DAG runs again → pulls same data → saves duplicates
  Model trains on duplicated data → wrong results

How Airflow handles this:
  catchup=False prevents historical re-runs
  Watermark tracking in pull_data task prevents
  duplicate data ingestion
  ON CONFLICT DO NOTHING in database inserts
  prevents duplicate rows
```

### Email Alerting — Why on_failure_callback Over email_on_failure

```
email_on_failure = True (Airflow built-in):
  Sends plain text email
  Only one recipient
  No customisation

on_failure_callback (custom function):
  Custom HTML email — formatted, readable
  Multiple recipients
  Include task logs in email
  Add Slack notification alongside email
  Different alerts for different failure types
  Conditional logic — only alert on critical tasks

Production systems use on_failure_callback
Built-in email_on_failure is for quick prototypes
```

---

*Week 9 of 15 · Apache Airflow ML Orchestration · Built in Nairobi, Kenya 🇰🇪*
*Airflow UI: http://52.57.213.229:8080 · Repository: https://github.com/M20Jay/airflow-mlops-pipeline*
*Part of a 15-week MLOps programme building production ML systems from scratch.*
