# Payment Processing Pipeline

A Kafka-based payment workflow demo with FastAPI services, PostgreSQL, Prometheus, and a React dashboard.

## What it does

- Produces payment events from the producer API
- Validates and fraud-checks them in Kafka consumers
- Writes successful transactions to PostgreSQL
- Sends notifications for settled/fraud outcomes
- Routes failed messages to a DLQ for admin inspection and replay
- Exposes metrics and overview data to a dashboard

## Flow

```text
Producer -> payments.raw -> Fraud -> payments.validated -> Ledger -> payments.settled -> Notification
                              \-> payments.fraud -> Notification
                              \-> payments.dlq -> Admin replay
```

## Run it

```bash
docker compose up --build -d
```

Frontend:

```bash
cd frontend
npm install
npm run dev
```

Open:

- Producer API: http://localhost:8000
- Admin API: http://localhost:8004
- Dashboard API: http://localhost:8005
- Prometheus: http://localhost:9090
- Frontend: http://localhost:5173

## Example request

```bash
curl -X POST http://localhost:8000/payments \
  -H "Content-Type: application/json" \
  -d '{
    "payment_id": "p-1001",
    "amount": 125.50,
    "currency": "USD",
    "customer_id": "cust-42",
    "merchant_id": "merchant-7"
  }'
```

## DLQ

Failed fraud/ledger messages are sent to `payments.dlq` and can be reviewed here:

```bash
curl http://localhost:8004/dlq
curl http://localhost:8004/dlq/<payment_id>
curl -X POST http://localhost:8004/dlq/replay/<payment_id>
```

## Test

```bash
python -m unittest discover -s tests -v
```

## Stop

```bash
docker compose down -v
```

