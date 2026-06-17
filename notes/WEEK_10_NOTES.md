# Week 10 — AWS Deep Dive

## What We Built
Complete AWS production infrastructure connecting the Airflow pipeline to permanent cloud services.

## The Problem Week 10 Solved
| Before Week 10 | After Week 10 |
|----------------|---------------|
| IP changed every restart | Permanent IP 3.67.15.230 |
| http://3.67.15.230:8080 | https://airflow.martin-mlops.com |
| Cloudflare tunnel expired randomly | Permanent HTTPS domain |
| Models stored on EC2 disk | S3 versioned artifact storage |
| Docker PostgreSQL no backups | RDS managed PostgreSQL |

## Day 1 — Elastic IP
- Allocated permanent IP: 3.67.15.230
- Attached to EC2 instance i-0b3c0fda7da6ccb95
- Never changes on restart

## Day 2 — Domain Name + Route 53
- Registered: martin-mlops.com via AWS Route 53
- A records pointing all subdomains to 3.67.15.230
- DNS propagation confirmed globally

## Day 3 — Nginx + HTTPS
- Nginx installed as reverse proxy
- SSL via Certbot + Let's Encrypt
- All services on HTTPS with green padlock

## Permanent URLs
| Service | URL |
|---------|-----|
| Airflow UI | https://airflow.martin-mlops.com |
| Air Quality API | https://api.martin-mlops.com/docs |
| Air Quality Dashboard | https://dashboard.martin-mlops.com |
| Recommendation Dashboard | https://recommendation.martin-mlops.com |

## Day 4 — S3 Model Storage
- Bucket: martin-mlops-models (eu-central-1 Frankfurt)
- IAM user: mlops-server with AmazonS3FullAccess
- Models saved with timestamps: model_20260609_080929.txt
- DAG updated: added save_model_to_s3 as 4th task

## Day 5 — RDS PostgreSQL
- Instance: martin-mlops-db (db.t4g.micro)
- PostgreSQL 18.3 with SSL TLSv1.3
- Database: mlops_db
- Table: model_runs (id, model_name, rmse, run_date)
- DAG updated: added save_metrics_to_rds as 5th task

## Day 6 — End to End Test
All 5 pipeline tasks ran green:
pull_data > retrain_model > evaluate_model > save_model_to_s3 > save_metrics_to_rds

RDS verified:
- id 3 — ARIMA 9.93 — 2026-06-17 (today)
- id 1 — ARIMA 9.93 — 2026-06-10 (Day 5 test)
- id 2 — LSTM 19.46 — 2026-06-10 (Day 5 test)

## AWS Resources Summary
| Resource | Details |
|----------|---------|
| EC2 | i-0b3c0fda7da6ccb95, t3.medium, Frankfurt |
| Elastic IP | 3.67.15.230 (permanent) |
| S3 Bucket | martin-mlops-models |
| RDS | martin-mlops-db.cdwsk8sk6aa2.eu-central-1.rds.amazonaws.com |
| Domain | martin-mlops.com (Route 53) |

## Key Lessons
- Elastic IP must be attached to a running instance to be free
- Nginx routes by domain name — one server, many services
- S3 versioning prevents model loss if EC2 is terminated
- RDS provides automatic daily backups — Docker PostgreSQL does not
- Let's Encrypt certificates renew automatically every 90 days

## Week 10 of 15 — AWS Deep Dive — Built in Nairobi, Kenya
Part of a 15-week MLOps programme building production ML systems from scratch.
github.com/M20Jay
