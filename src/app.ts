import xxhash from "xxhash-wasm";
import { Queue, Worker, QueueEvents } from "bullmq";
import { Redis } from "ioredis";
import { createClient } from "@clickhouse/client";
import { config } from "dotenv";
import express from "express";
import { getRawResult, setResult } from "./utils/cache-client";
import {
  RequestPayload as GasUsageRequestPayload,
  handleGasUsage,
  isRequestPayload as isGasUsageRequestPayload,
} from "./adapters/contracts/gas-usage";
import {
  RequestPayload as TotalTxsRequestPayload,
  handleTotalTxs,
  isRequestPayload as isTotalTxsRequestPayload,
} from "./adapters/contracts/total-txs";
import {
  RequestPayload as TotalUniqueUsersRequestPayload,
  handleTotalUniqueUsers,
  isRequestPayload as isTotalUniqueUsersRequestPayload,
} from "./adapters/contracts/total-unique-users";
import { toHash } from "./utils/convert";
import { isSupportedChain } from "./types";
import { Hex } from "viem";

config();

export class AppContext {
  hasher?: (input: string, seed?: bigint | undefined) => string;

  constructor() {
    xxhash().then((xxhash) => {
      this.hasher = xxhash.h64ToString;
      console.log("xxhash loaded");
    });
  }
}
export const appContext = new AppContext();

const REDIS_URL = process.env.REDIS_URL as string;
const redis = new Redis(REDIS_URL);
const clickhouse = createClient({
  host: process.env.CLICKHOUSE_HOST,
  username: process.env.CLICKHOUSE_USERNAME,
  password: process.env.CLICKHOUSE_PASSWORD,
});

interface QueryJob {
  id: string;
  adapter: string;
  req: any;
}

const queryQueue = new Queue<QueryJob>("queryQueue", { connection: redis });
const queryQueueEvents = new QueueEvents("queryQueue", { connection: redis });

new Worker<QueryJob>(
  "queryQueue",
  async ({ data }) => {
    const { adapter, id, req } = data;
    let result;

    switch (adapter) {
      case "contracts:gas-usage":
        result = await handleGasUsage(req, clickhouse);
        await setResult(id, result);
        break;
      case "contracts:total-txs":
        result = await handleTotalTxs(req, clickhouse);
        await setResult(id, result);
        break;
      case "contracts:total-unique-users":
        result = await handleTotalUniqueUsers(req, clickhouse);
        await setResult(id, result);
        break;
      default:
        throw new Error("unknown adapter");
    }
  },
  { connection: redis, concurrency: 20 },
);

const app = express();

app.use(express.json());
app.use((req, res, next) => {
  const token = req.header("X-Auth-Token");
  if (token !== process.env.AUTH_TOKEN) {
    res.status(401).json({ error: "unauthorized" });
    return;
  }
  next();
});

app.post("/contracts/gas-usage", async (req, res) => {
  const adapter = "contracts:gas-usage";
  if (!isGasUsageRequestPayload(req.body)) {
    res.status(400).json({ error: "400" });
    return;
  }

  try {
    const { contracts, startTimestamp, endTimestamp } = req.body;
    const _contracts = contracts
      .filter((c) => isSupportedChain(c.chain))
      .map(({ address, chain }) => ({ chain, address: address.toLowerCase() as Hex }));
    // round to 30mins
    const _startTimestamp = Math.floor(startTimestamp / 1800) * 1800;
    const _endTimestamp = Math.floor(endTimestamp / 1800) * 1800;

    const _req = {
      contracts: _contracts,
      startTimestamp: _startTimestamp,
      endTimestamp: _endTimestamp,
    } as GasUsageRequestPayload;

    const id = adapter + ":" + toHash(_req, appContext.hasher!);
    const cache = await getRawResult(id);
    if (cache) {
      console.log("[cache hit] " + id);
      res.contentType("json").send(cache);
      return;
    }
    console.log("[cache miss] " + id);

    const job = await queryQueue.add(
      id,
      { id, adapter, req: _req },
      { removeOnComplete: true, removeOnFail: 1000, attempts: 3 },
    );
    console.log("[job added] " + id);

    await job.waitUntilFinished(queryQueueEvents);
    console.log("[job finished] " + id);

    const result = await getRawResult(id);
    if (!result) {
      // means it errored out
      res.status(400).json({ error: "400" });
      return;
    }

    res.contentType("json").send(result);
  } catch (e) {
    console.log(e);
    res.status(500).json({ error: "500" });
  }
});

app.post("/contracts/total-txs", async (req, res) => {
  const adapter = "contracts:total-txs";
  if (!isTotalTxsRequestPayload(req.body)) {
    res.status(400).json({ error: "400" });
    return;
  }

  try {
    const { contracts, startTimestamp, endTimestamp } = req.body;
    const _contracts = contracts
      .filter((c) => isSupportedChain(c.chain))
      .map(({ address, chain }) => ({ chain, address: address.toLowerCase() as Hex }));
    // round to 30mins
    const _startTimestamp = Math.floor(startTimestamp / 1800) * 1800;
    const _endTimestamp = Math.floor(endTimestamp / 1800) * 1800;

    const _req = {
      contracts: _contracts,
      startTimestamp: _startTimestamp,
      endTimestamp: _endTimestamp,
    } as TotalTxsRequestPayload;

    const id = adapter + ":" + toHash(_req, appContext.hasher!);
    const cache = await getRawResult(id);
    if (cache) {
      console.log("[cache hit] " + id);
      res.contentType("json").send(cache);
      return;
    }
    console.log("[cache miss] " + id);

    const job = await queryQueue.add(
      id,
      { id, adapter, req: _req },
      { removeOnComplete: true, removeOnFail: 1000, attempts: 3 },
    );
    console.log("[job added] " + id);

    await job.waitUntilFinished(queryQueueEvents);
    console.log("[job finished] " + id);

    const result = await getRawResult(id);
    if (!result) {
      // means it errored out
      res.status(400).json({ error: "400" });
      return;
    }

    res.contentType("json").send(result);
  } catch (e) {
    console.log(e);
    res.status(500).json({ error: "500" });
  }
});

app.post("/contracts/total-unique-users", async (req, res) => {
  const adapter = "contracts:total-unique-users";
  if (!isTotalUniqueUsersRequestPayload(req.body)) {
    res.status(400).json({ error: "400" });
    return;
  }

  try {
    const { contracts, startTimestamp, endTimestamp } = req.body;
    const _contracts = contracts
      .filter((c) => isSupportedChain(c.chain))
      .map(({ address, chain }) => ({ chain, address: address.toLowerCase() as Hex }));
    // round to 30mins
    const _startTimestamp = Math.floor(startTimestamp / 1800) * 1800;
    const _endTimestamp = Math.floor(endTimestamp / 1800) * 1800;

    const _req = {
      contracts: _contracts,
      startTimestamp: _startTimestamp,
      endTimestamp: _endTimestamp,
    } as TotalUniqueUsersRequestPayload;

    const id = adapter + ":" + toHash(_req, appContext.hasher!);
    const cache = await getRawResult(id);
    if (cache) {
      console.log("[cache hit] " + id);
      res.contentType("json").send(cache);
      return;
    }
    console.log("[cache miss] " + id);

    const job = await queryQueue.add(
      id,
      { id, adapter, req: _req },
      { removeOnComplete: true, removeOnFail: 1000, attempts: 3 },
    );
    console.log("[job added] " + id);

    await job.waitUntilFinished(queryQueueEvents);
    console.log("[job finished] " + id);

    const result = await getRawResult(id);
    if (!result) {
      // means it errored out
      res.status(400).json({ error: "400" });
      return;
    }

    res.contentType("json").send(result);
  } catch (e) {
    console.log(e);
    res.status(500).json({ error: "500" });
  }
});

app.all("*", (_, res) => {
  res.status(404).json({ error: "404" });
});

app.listen(process.env.PORT || 3000, () => {
  console.log("server started");
});
