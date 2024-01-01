import { ClickHouseClient } from "@clickhouse/client";
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
    txs: number;
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
  totalTxs: number;
  /** In seconds */
  startTimestamp: number;
  /** In seconds */
  endTimestamp: number;
};

/**
 * Get total transaction count for a list of contracts on a chain within a time range.
 *
 * @param params - Params object with chain, addresses, startTimestamp, and endTimestamp
 * @param client - ClickHouse client (optional)
 * @returns
 */
export const getTotalTxsSingleChain = async (params: Params, client: ClickHouseClient): Promise<Result> => {
  const { chain, addresses, startTimestamp, endTimestamp } = params;
  const start = formatDate(new Date(startTimestamp * 1000));
  const end = formatDate(new Date(endTimestamp * 1000));

  const query = sql`
    SELECT
      COUNT(*) AS total_txs
    FROM
      ${chain}.base_transactions bt
    WHERE
      bt.to_address IN (${addresses.map((address) => `'${address}'`).join(", ")})
      AND bt.block_timestamp BETWEEN '${start}' AND '${end}'
    ;
  `;

  const resultSet = await client.query({ query, format: "JSONEachRow" });
  const dataset = await resultSet.json<{ total_txs: string }[]>();
  return {
    chain,
    totalTxs: Number(dataset[0]?.total_txs ?? "0"),
    startTimestamp,
    endTimestamp,
  };
};

export const handleTotalTxs = async (req: RequestPayload, client: ClickHouseClient): Promise<ResponsePayload> => {
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
    getTotalTxsSingleChain({ chain: chain as SupportedChain, addresses, startTimestamp, endTimestamp }, client),
  );
  const res = await Promise.all(reqs);

  return {
    total: res.map((r) => ({
      chain: r.chain,
      txs: r.totalTxs,
    })),
    startTimestamp,
    endTimestamp,
  };
};
