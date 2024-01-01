<p align="center">
<img align="center" width="128" src="./logo.png">
</p>
<h1 align="center">
<strong>Indexa API</strong>
</h1>
<p align="center">
<strong>A simple web service for standardized blockchain data.</strong>
</p>

**WIP** - This is a work in progress, currently only to be used internally at DefiLlama.

## Background

This is a RESTful API server that queries and caches blockchain data from predefined queries. The underlying data warehouse service is the Indexa V2 clickhouse database.

The server caches queries aggressively to reduce the load on the database. The cache is currently invalidated every 24 hours. If the request param has a time span, the starting/ending time will be rounded to the previous hour. The response will contain the rounded timestamps. All timestamps are in Unix seconds, **not** milliseconds.

The concurrency of the database queries are managed by a Bullmq job queue.

Currently, the server also requires a custom header `X-Auth-Token` to be set for internal usage.

## API

Note that `type SupportedChain = 'ethereum' | 'bsc' | 'arbitrum'`

### Contract Interaction Queries

#### POST `/contracts/gas-usage`

Get total gas usage for a list of contracts on a chain within a time range.

**Request body**

```ts
{
  contracts: Array({
    chain: SupportedChain;
    address: Hex;
  });
  startTimestamp: number;
  endTimestamp: number;
}
```

**Response body**

```ts
{
  chain: SupportedChain;
  /** In wads */
  totalGasUsage: number;
  startTimestamp: number;
  endTimestamp: number;
}
```

#### POST `/contracts/total-txs`

Get total transaction count for a list of contracts on a chain within a time range.

**Request body**

```ts
{
  contracts: Array({
    chain: SupportedChain;
    address: Hex;
  });
  startTimestamp: number;
  endTimestamp: number;
}
```

**Response body**

```ts
{
  total: Array({
    chain: SupportedChain;
    txs: number;
  });
  startTimestamp: number;
  endTimestamp: number;
}
```

#### POST `/contracts/total-unique-users`

Get total unique users who have interacted with the contract directly, given a list of contracts on a chain within a time range.

**Request body**

```ts
{
  contracts: Array({
    chain: SupportedChain;
    address: Hex;
  });
  startTimestamp: number;
  endTimestamp: number;
}
```

**Response body**

```ts
{
  total: Array({
    chain: SupportedChain;
    users: number;
  });
  startTimestamp: number;
  endTimestamp: number;
}
```
