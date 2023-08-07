import { createClient } from "@clickhouse/client";
import { Hex } from "viem";
import { sql } from "../../utils/db";
import { SupportedChain, isSupportedChain } from "../../types";
import { formatDate } from "../../utils/convert";

export type RequestPayload = {
  contracts: {
    chain: SupportedChain;
    /** In lowercase */
    address: Hex;
  }[];
  /** In seconds */
  startTimestamp: number;
  /** In seconds */
  endTimestamp: number;
};
export const isRequestPayload = (payload: any): payload is RequestPayload => {
  try {
    const { contracts, startTimestamp, endTimestamp } = payload;
    if (!contracts || !Array.isArray(contracts) || contracts.length === 0) return false;
    if (!startTimestamp || typeof startTimestamp !== "number") return false;
    if (!endTimestamp || typeof endTimestamp !== "number") return false;
    return true;
  } catch (e) {
    return false;
  }
};

export type ResponsePayload = {
  total: {
    chain: SupportedChain;
    /** In wads */
    gasUsage: number;
  }[];
  /** In seconds */
  startTimestamp: number;
  /** In seconds */
  endTimestamp: number;
};

export type Params = {
  chain: SupportedChain;
  /** In lowercase */
  addresses: readonly Hex[];
  /** In seconds */
  startTimestamp: number;
  /** In seconds */
  endTimestamp: number;
};

export type Result = {
  chain: SupportedChain;
  /** In wads */
  totalGasUsage: number;
  /** In seconds */
  startTimestamp: number;
  /** In seconds */
  endTimestamp: number;
};

/**
 * Get total gas usage for a list of contracts on a chain within a time range.
 *
 * @param params - Params object with chain, addresses, startTimestamp, and endTimestamp
 * @param client - ClickHouse client (optional)
 * @returns
 */
export const getTotalGasUsageSingleChain = async (
  params: Params,
  client = createClient({
    host: process.env.CLICKHOUSE_HOST,
    username: process.env.CLICKHOUSE_USERNAME,
    password: process.env.CLICKHOUSE_PASSWORD,
  }),
): Promise<Result> => {
  const { chain, addresses, startTimestamp, endTimestamp } = params;
  const start = formatDate(new Date(startTimestamp * 1000));
  const end = formatDate(new Date(endTimestamp * 1000));

  const query = sql`
    SELECT
      SUM(bt.gas_used * bt.gas_price / 1e18) AS total_gas_usage
    FROM
      ${chain}.base_transactions bt
    WHERE
      bt.to_address IN (${addresses.map((address) => `'${address}'`).join(", ")})
      AND bt.block_timestamp BETWEEN '${start}' AND '${end}'
    ;
  `;

  const resultSet = await client.query({ query, format: "JSONEachRow" });
  const dataset = await resultSet.json<{ total_gas_usage: number }[]>();
  return {
    chain,
    totalGasUsage: dataset[0].total_gas_usage,
    startTimestamp,
    endTimestamp,
  };
};

export const handleGasUsage = async (
  req: RequestPayload,
  client = createClient({
    host: process.env.CLICKHOUSE_HOST,
    username: process.env.CLICKHOUSE_USERNAME,
    password: process.env.CLICKHOUSE_PASSWORD,
  }),
): Promise<ResponsePayload> => {
  const { contracts, startTimestamp, endTimestamp } = req;
  const supportedContracts = contracts.filter((contract) => isSupportedChain(contract.chain));
  const contractDicts = supportedContracts.reduce<Record<SupportedChain, Hex[]>>((acc, contract) => {
    if (!acc[contract.chain]) {
      acc[contract.chain] = [];
    }
    acc[contract.chain].push(contract.address);
    return acc;
  }, {} as Record<SupportedChain, Hex[]>);

  const reqs = Object.entries(contractDicts).map(([chain, addresses]) =>
    getTotalGasUsageSingleChain({ chain: chain as SupportedChain, addresses, startTimestamp, endTimestamp }, client),
  );
  const res = await Promise.all(reqs);

  return {
    total: res.map((r) => ({
      chain: r.chain,
      gasUsage: r.totalGasUsage,
    })),
    startTimestamp,
    endTimestamp,
  };
};
