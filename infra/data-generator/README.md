# Data Generator

Realistic e-commerce data generator that simulates user activity, orders, and product catalog updates. It inserts data directly into PostgreSQL, which acts as the source for downstream CDC processing.

## Configuration

Core settings are managed via the root `.env` and `docker-compose.yaml` files. The generator produces events for:
- Users
- Products
- Orders
- Order Items
- Events / Sessions

## Running

This service is part of the `datagen` profile.

```bash
# Start just the data generator along with core services
make up-datagen
```
