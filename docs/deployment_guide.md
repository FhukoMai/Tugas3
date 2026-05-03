# Deployment Guide

## Prerequisites
- Docker
- Docker Compose

## Menjalankan Sistem
1. cd ke direktori `docker`.
2. jalankan perintah `docker-compose up --build -d`.
3. nanti akan menjalankan 3 Node dan juga prometheus
4. Prometheusnya `http://localhost:9090`.

## Swagger / OpenAPI Docs
Ini Node Nodenya:
- Node 1: `http://localhost:8001/docs`
- Node 2: `http://localhost:8002/docs`
- Node 3: `http://localhost:8003/docs`