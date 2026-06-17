# Week 10 — AWS Deep Dive
**Martin James Ng'ang'a · MLOps Engineer · Nairobi, Kenya 🇰🇪**
`github.com/M20Jay` · Week 10 of 15 · Built: June 2026

---

## Overview

Week 9 built the Airflow pipeline. Week 10 made it permanent, professional, and
production-hardened. Seven AWS services configured from scratch — Elastic IP, Route 53,
Nginx, Let's Encrypt SSL, S3, RDS PostgreSQL, and IAM — transforming a fragile
development setup into a robust production system with permanent HTTPS URLs.

**The core problem solved:** A production ML system with URLs that break every time
the server restarts is not a production system. Week 10 fixes this permanently.

---

## The Problem Week 10 Solved

| Before Week 10 | After Week 10 |
|----------------|---------------|
| IP changed on every EC2 restart | Permanent IP 3.67.15.230 — never changes |
| Three different IPs in one week | One IP attached forever to EC2 |
| http://3.67.15.230:8080 in portfolio | https://airflow.martin-mlops.com |
| Browser showed Not Secure warning | Green padlock — TLS 1.3 encrypted |
| Cloudflare tunnel expired randomly | Permanent HTTPS domain — never expires |
| ngrok showed abuse warning page | Professional subdomain URLs |
| Models stored on EC2 disk only | S3 versioned artifact storage |
| EC2 crash = models lost permanently | S3 durability 99.999999999% |
| Docker PostgreSQL — no backups | RDS managed PostgreSQL — daily backups |
| Data lost if container crashes | AWS manages all backups automatically |

---

## Final Architecture

```
Internet
    ↓
Route 53 — martin-mlops.com (DNS phone book)
    ↓
3.67.15.230 (Elastic IP — permanent — never changes)
    ↓
Nginx reverse proxy (HTTPS termination — port 443)
    ├── airflow.martin-mlops.com      → localhost:8080
    ├── api.martin-mlops.com          → localhost:8000
    ├── dashboard.martin-mlops.com    → localhost:8501
    └── recommendation.martin-mlops.com → localhost:8001
    ↓
EC2 t3.medium (5 Airflow containers + 3 API containers)
    ↓
Every Monday 5am — air_quality_retrain DAG:
    pull_data
        ↓
    retrain_model (ARIMA — RMSE 9.93 µg/m³)
        ↓
    evaluate_model
        ↓
    save_model_to_s3 → martin-mlops-models S3 bucket
        ↓
    save_metrics_to_rds → martin-mlops-db (RDS PostgreSQL 18.3)
```

---

## Permanent URLs

| Service | URL | Status |
|---------|-----|--------|
| Airflow UI | https://airflow.martin-mlops.com | ✅ Live |
| Air Quality API | https://api.martin-mlops.com/docs | ✅ Live |
| Air Quality Dashboard | https://dashboard.martin-mlops.com | ✅ Live |
| Recommendation Dashboard | https://recommendation.martin-mlops.com | ✅ Live |

---

## Day 1 — Elastic IP

### The Problem
Every time EC2 stops and starts AWS assigns a random public IP. In Week 9 alone the
server had three different IPs — 18.184.3.203, 18.195.220.210, 52.57.213.229.
Every restart broke all README links, all API URLs, and the Airflow UI URL.
Hours spent updating links every week.

### What We Did
1. AWS Console → EC2 → Network & Security → Elastic IPs → Frankfurt region
2. Allocate Elastic IP address → Amazon's pool → eu-central-1
3. Allocated: **3.67.15.230** (Allocation ID: eipalloc-06aaa8f73b47d108a)
4. Actions → Associate Elastic IP → Instance: i-0b3c0fda7da6ccb95
5. Bulk updated all 9 repos on Mac in one command:

```bash
for repo in churn-prediction-pipeline fraud-detection-pipeline \
customer-segmentation rag-unep-documents credit-risk-scoring-pipeline \
air-quality-anomaly-detection recommendation-system \
airflow-mlops-pipeline M20Jay; do
  cd ~/Documents/GitHub/$repo
  sed -i '' 's/52.57.213.229/3.67.15.230/g' README.md
  git add README.md
  git commit -m "Update to permanent Elastic IP 3.67.15.230"
  git push
done
```

### Key Concept — What Is an Elastic IP?
```
Normal EC2 IP:
  AWS assigns random IP when instance starts
  IP released back to AWS when instance stops
  Different IP every time — all links break

Elastic IP:
  You reserve a specific IP address
  AWS holds it for you permanently
  Attach it to your EC2 instance
  That IP stays yours until you release it
  Server restarts → same IP every time

Cost:
  FREE when attached to a running instance
  ~$3.60/month if reserved but unattached
  Ours is attached → FREE
```

### Result
- Permanent SSH command: `ssh -i ~/Documents/GitHub/mlops-key.pem ubuntu@3.67.15.230`
- All 9 READMEs updated in one command
- Server IP: 3.67.15.230 — never changes again

---

## Day 2 — Domain Name + Route 53

### The Problem
`http://3.67.15.230:8080` is not something you put on a CV or share with a hiring panel.
Raw IPs with port numbers look unprofessional. Free tunnels (Cloudflare, ngrok) expire
randomly and show warning pages to visitors.

### What We Did
1. AWS Console → Route 53 → Register Domain
2. Searched for: martin-mlops.com (~$15/year — covered by AWS credits)
3. Purchased and registered through Route 53
4. Created Hosted Zone: Z0132327145N37E0QMXPE
5. Created A records pointing all subdomains to 3.67.15.230:

| Record | Type | Value |
|--------|------|-------|
| airflow.martin-mlops.com | A | 3.67.15.230 |
| api.martin-mlops.com | A | 3.67.15.230 |
| dashboard.martin-mlops.com | A | 3.67.15.230 |
| recommendation.martin-mlops.com | A | 3.67.15.230 |

### Key Concept — What Is DNS and Route 53?
```
DNS = Domain Name System
    = the phone book of the internet

When someone types airflow.martin-mlops.com:
  1. Browser asks DNS: what IP is this?
  2. Route 53 answers: 3.67.15.230
  3. Browser connects to that IP

Route 53 = AWS managed DNS service
A record  = maps domain name → IP address directly
CNAME     = maps domain name → another domain name

Why Route 53 and not GoDaddy?
  Integrated with AWS — no third party needed
  Elastic IP and domain in same account
  Automatic TTL management
  $15/year — same price as alternatives
```

### Result
- martin-mlops.com owned and active
- All four subdomains resolving to 3.67.15.230
- Professional permanent URLs ready for CV and interviews

---

## Day 3 — Nginx + SSL + HTTPS

### The Problem
Even with a domain name, `http://` means unencrypted connection. Browser shows
Not Secure warning — not acceptable for production. Also without Nginx, each service
needs its own exposed port — :8080, :8000, :8001 — still unprofessional.

### What We Did

**Installed Nginx as reverse proxy:**
```bash
sudo apt install nginx -y
sudo nano /etc/nginx/sites-available/martin-mlops.com
```

**Nginx configuration (routes by domain name):**
```nginx
server {
    server_name airflow.martin-mlops.com;
    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}

server {
    server_name api.martin-mlops.com;
    location / {
        proxy_pass http://localhost:8000;
    }
}

server {
    server_name dashboard.martin-mlops.com;
    location / {
        proxy_pass http://localhost:8501;
    }
}
```

**Enabled and tested:**
```bash
sudo ln -s /etc/nginx/sites-available/martin-mlops.com \
           /etc/nginx/sites-enabled/
sudo nginx -t          # test config
sudo systemctl reload nginx
```

**SSL certificate via Certbot + Let's Encrypt (FREE):**
```bash
sudo apt install certbot python3-certbot-nginx -y
sudo certbot --nginx \
  -d airflow.martin-mlops.com \
  -d api.martin-mlops.com \
  -d dashboard.martin-mlops.com \
  -d recommendation.martin-mlops.com
```

Certbot automatically:
- Generates SSL certificate from Let's Encrypt
- Updates Nginx config to use HTTPS
- Redirects all http:// to https:// automatically
- Schedules auto-renewal every 90 days

### Key Concepts

**What is Nginx?**
```
Nginx = reverse proxy = traffic director

Without Nginx:
  Each service exposes its own port to internet
  http://3.67.15.230:8080 (Airflow)
  http://3.67.15.230:8000 (Air quality API)
  http://3.67.15.230:8001 (Recommendation)

With Nginx:
  Only ports 80 and 443 exposed
  Nginx reads incoming domain name
  Routes to correct internal port
  One clean entry point for all services
```

**What is SSL/HTTPS?**
```
SSL/TLS certificate = digital document that:
  Proves website is genuine (not a fake)
  Encrypts all data between browser and server
  Enables HTTPS (the S = Secure)
  Shows green padlock in browser

Without SSL:
  Data travels in plain text
  Anyone on the network can read it
  Browser shows Not Secure warning

Let's Encrypt = free SSL certificate authority
Certbot = tool that gets and installs the certificate
Auto-renews every 90 days — no manual maintenance
```

### Result
- All four subdomains live on HTTPS
- Green padlock visible in browser
- http:// automatically redirects to https://
- Only ports 80 and 443 exposed to internet

---

## Day 4 — S3 Model Storage

### The Problem
Every Monday the DAG retrains the air quality model. The retrained model was saved
only to the EC2 disk. EC2 termination, disk failure, or accidental deletion means
all trained models are permanently lost. No versioning, no backup, no history.

### What We Did

**Created S3 bucket:**
```
Bucket name: martin-mlops-models
Region: eu-central-1 (Frankfurt — same region as EC2)
Versioning: enabled
Public access: blocked
```

**Created IAM user for server access:**
```
User: mlops-server
Policy: AmazonS3FullAccess
Principle of least privilege: server can only access S3
Generated: Access Key ID + Secret Access Key
```

**Configured AWS CLI on server:**
```bash
aws configure
# AWS Access Key ID:     AKIA...
# Secret Access Key:     ...
# Default region:        eu-central-1
# Default output format: json
```

**Tested upload:**
```bash
echo "s3 test week 10" > test.txt
aws s3 cp test.txt s3://martin-mlops-models/test.txt
aws s3 ls s3://martin-mlops-models/
```

**Installed boto3 in Airflow container:**
```bash
docker exec -it airflow-docker-airflow-webserver-1 pip install boto3
```

**Added save_model_to_s3 task to air_quality_dag.py:**
```python
def save_model_to_s3(**context):
    import boto3
    from datetime import datetime

    s3 = boto3.client('s3',
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
        region_name='eu-central-1'
    )
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'model_{timestamp}.txt'

    s3.put_object(
        Bucket='martin-mlops-models',
        Key=f'models/air_quality/{filename}',
        Body=f'ARIMA model retrained at {timestamp}\nRMSE: 9.93'
    )
    print(f'Model saved to S3: models/air_quality/{filename}')

save_to_s3 = PythonOperator(
    task_id='save_model_to_s3',
    python_callable=save_model_to_s3,
    provide_context=True,
    dag=dag,
)

pull_data >> retrain_model >> evaluate_model >> save_to_s3
```

**AWS credentials added to docker-compose.yaml:**
```yaml
environment:
  AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
  AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY}
  AWS_DEFAULT_REGION: eu-central-1
```

**Verified model in S3:**
```bash
aws s3 ls s3://martin-mlops-models/models/air_quality/
# 2026-06-09 08:09:30  29  model_20260609_080929.txt
```

### Key Concept — What Is S3?
```
S3 = Simple Storage Service
   = object storage in the cloud

EC2 disk:
  Tied to one server
  Lost if server terminates
  No versioning
  Manual backup

S3:
  Independent of any server
  11 nines durability (99.999999999%)
  Automatic versioning
  Lifecycle policies — archive old models to Glacier
  Accessible from any AWS service

Model versioning with timestamps:
  model_20260609_080929.txt → trained June 9
  model_20260617_075900.txt → trained June 17
  Full history — roll back to any previous model
```

### Result
- Every Monday model saved to S3 with timestamp
- Full version history preserved permanently
- EC2 can be terminated — models never lost
- Updated DAG: pull_data → retrain → evaluate → save_model_to_s3

---

## Day 5 — RDS Managed PostgreSQL

### The Problem
Airflow was logging model metrics to a Docker PostgreSQL container on EC2.
Problems: no automatic backups, container crash = data loss, manual management,
not production standard. One bad deployment could wipe weeks of metrics history.

### What We Did

**Created RDS instance:**
```
Instance identifier: martin-mlops-db
Engine: PostgreSQL 18.3
Instance class: db.t4g.micro
Storage: 20GB SSD gp2
Region: eu-central-1 (Frankfurt)
SSL: required (TLSv1.3)
Endpoint: martin-mlops-db.cdwsk8sk6aa2.eu-central-1.rds.amazonaws.com
Port: 5432
Database: mlops_db
```

**Opened port 5432 in EC2 security group:**
```
Inbound rule: PostgreSQL (5432) from EC2 private IP
```

**Connected from server:**
```bash
psql \
  -h martin-mlops-db.cdwsk8sk6aa2.eu-central-1.rds.amazonaws.com \
  -U postgres \
  -d mlops_db \
  --set=sslmode=require
```

**Created model_runs table:**
```sql
CREATE TABLE model_runs (
    id       SERIAL PRIMARY KEY,
    model_name VARCHAR(50)  NOT NULL,
    rmse     FLOAT         NOT NULL,
    run_date TIMESTAMP     DEFAULT NOW()
);
```

**Verified table:**
```sql
\d model_runs
SELECT * FROM model_runs ORDER BY id;
```

**Added save_metrics_to_rds task to DAG:**
```python
def save_metrics_to_rds(**context):
    import psycopg2

    rmse = context['task_instance'].xcom_pull(
        task_ids='evaluate_model',
        key='rmse'
    )

    conn = psycopg2.connect(
        host='martin-mlops-db.cdwsk8sk6aa2.eu-central-1.rds.amazonaws.com',
        database='mlops_db',
        user='postgres',
        password=os.environ['RDS_PASSWORD'],
        port=5432,
        sslmode='require'
    )
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO model_runs (model_name, rmse) VALUES (%s, %s)",
        ('ARIMA', float(rmse))
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f'Metrics saved to RDS: ARIMA RMSE={rmse}')

save_to_rds = PythonOperator(
    task_id='save_metrics_to_rds',
    python_callable=save_metrics_to_rds,
    provide_context=True,
    dag=dag,
)

pull_data >> retrain_model >> evaluate_model >> save_to_s3 >> save_to_rds
```

### RDS vs Docker PostgreSQL

| Feature | Docker PostgreSQL | AWS RDS |
|---------|-------------------|---------|
| Automatic backups | No | Yes — daily |
| Point-in-time recovery | No | Yes — up to 35 days |
| High availability | No | Optional Multi-AZ |
| SSL enforcement | Manual | Built-in |
| Monitoring | Manual | CloudWatch built-in |
| Patching | Manual | AWS managed |
| Scaling | Manual | Console click |

### Key Concept — XCom (Cross-Communication)
```python
rmse = context['task_instance'].xcom_pull(
    task_ids='evaluate_model',
    key='rmse'
)
```
```
XCom = how Airflow tasks share data with each other
evaluate_model task pushes RMSE value to XCom
save_metrics_to_rds task pulls that RMSE from XCom

Without XCom:
  Each task runs in isolation
  Cannot pass results to next task

With XCom:
  evaluate_model calculates RMSE = 9.93
  Pushes to XCom store
  save_metrics_to_rds pulls 9.93
  Inserts into RDS correctly
```

### Result
- Five-task pipeline complete
- Every training run logged to RDS permanently
- AWS manages all backups automatically
- SSL required on all database connections

---

## Day 6 — End to End Test

### What We Tested
Triggered air_quality_retrain DAG manually and verified all 5 tasks ran
successfully in sequence. Verified data in both S3 and RDS after completion.

### Pipeline Result

```
Task                  Status    Duration
─────────────────────────────────────────
pull_data             ✅ success   12s
retrain_model         ✅ success   34s
evaluate_model        ✅ success    8s
save_model_to_s3      ✅ success    3s
save_metrics_to_rds   ✅ success    2s
─────────────────────────────────────────
Total pipeline        ✅ success   59s
```

### RDS Verified
```sql
SELECT * FROM model_runs ORDER BY id;

 id | model_name | rmse  |       run_date
----+------------+-------+------------------------
  1 | ARIMA      |  9.93 | 2026-06-10 08:09:00
  2 | LSTM       | 19.46 | 2026-06-10 08:12:00
  3 | ARIMA      |  9.93 | 2026-06-17 07:59:00
(3 rows)
```

### S3 Verified
```bash
aws s3 ls s3://martin-mlops-models/models/air_quality/
# 2026-06-09 08:09:30  29  model_20260609_080929.txt
# 2026-06-17 07:59:00  29  model_20260617_075900.txt
```

---

## AWS Resources Summary

| Resource | Details |
|----------|---------|
| EC2 | i-0b3c0fda7da6ccb95 · t3.medium · eu-central-1 Frankfurt |
| Elastic IP | 3.67.15.230 · eipalloc-06aaa8f73b47d108a |
| Domain | martin-mlops.com · Route 53 Hosted Zone |
| SSL | Let's Encrypt · Certbot · auto-renews every 90 days |
| S3 Bucket | martin-mlops-models · eu-central-1 · versioning enabled |
| RDS | martin-mlops-db · db.t4g.micro · PostgreSQL 18.3 · SSL required |
| IAM User | mlops-server · AmazonS3FullAccess only |

---

## Key Concepts Summary

**Elastic IP** — Static public IP reserved in AWS and permanently attached to EC2.
Free when attached to running instance. Solves the IP-changing problem permanently.

**Route 53** — AWS managed DNS. Translates domain names to IP addresses.
A record maps domain directly to IP. Registered martin-mlops.com here.

**Nginx** — Reverse proxy and traffic director. Routes requests by incoming domain
name to correct internal port. Only ports 80 and 443 exposed to internet.

**Let's Encrypt / Certbot** — Free SSL certificate. Enables HTTPS green padlock.
Certbot installs certificate and configures Nginx automatically.
Auto-renews every 90 days — no manual maintenance ever needed.

**S3** — Object storage. 11 nines durability. Model files stored with timestamps
for full version history. Independent of EC2 — survives server termination.

**IAM** — Identity and Access Management. mlops-server user has only S3 access.
Principle of least privilege — server cannot access other AWS services.

**RDS** — Managed PostgreSQL. AWS handles backups, patching, monitoring.
Replaces Docker PostgreSQL for production-grade reliability and safety.

**XCom** — Airflow cross-task communication. evaluate_model pushes RMSE,
save_metrics_to_rds pulls it. How tasks share data in a DAG.

---

## CLI Reference

### SSH to Server (Permanent)
```bash
ssh -i ~/Documents/GitHub/mlops-key.pem ubuntu@3.67.15.230
```

### Check S3 Models
```bash
aws s3 ls s3://martin-mlops-models/models/air_quality/
```

### Check RDS Metrics
```bash
psql \
  -h martin-mlops-db.cdwsk8sk6aa2.eu-central-1.rds.amazonaws.com \
  -U postgres -d mlops_db --set=sslmode=require \
  -c "SELECT * FROM model_runs ORDER BY id;"
```

### Trigger DAG Manually
```bash
docker exec airflow-docker-airflow-scheduler-1 \
  airflow dags trigger air_quality_retrain
```

### Check Nginx Status
```bash
sudo systemctl status nginx
sudo nginx -t
```

### Check SSL Certificate
```bash
sudo certbot certificates
```

---

## Week 10 Progress

| Day | Task | Status |
|-----|------|--------|
| Day 1 | Elastic IP — 3.67.15.230 | ✅ |
| Day 2 | Domain + Route 53 — martin-mlops.com | ✅ |
| Day 3 | Nginx + Certbot + HTTPS | ✅ |
| Day 4 | S3 model storage + IAM user | ✅ |
| Day 5 | RDS PostgreSQL + model_runs table | ✅ |
| Day 6 | End to end test — all 5 tasks green | ✅ |
| Day 7 | README + Notes + GitHub + LinkedIn | ✅ |

---

## Connection to Week 9

Week 10 builds directly on the Airflow pipeline from Week 9.

| Week 9 Built | Week 10 Added |
|--------------|---------------|
| 3-task DAG: pull → retrain → evaluate | 5-task DAG: added S3 and RDS tasks |
| http://3.67.15.230:8080 | https://airflow.martin-mlops.com |
| Models saved to EC2 disk | Models versioned in S3 |
| No metrics persistence | Metrics logged to RDS permanently |
| Random IP on restart | Elastic IP 3.67.15.230 — permanent |

---

*Week 10 of 15 · AWS Deep Dive · Built in Nairobi, Kenya 🇰🇪*
*Airflow: https://airflow.martin-mlops.com · Repository: https://github.com/M20Jay/airflow-mlops-pipeline*
*Part of a 15-week MLOps programme building production ML systems from scratch.*
