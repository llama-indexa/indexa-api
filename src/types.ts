import { ClickHouseClient } from "@clickhouse/client";

// prettier-ignore
export type IfEquals<T, U, Y=true, N=false> =
  (<G>() => G extends T ? 1 : 2) extends
  (<G>() => G extends U ? 1 : 2) ? Y : N;

export type SupportedChain = "bsc" | "ethereum";
export const isSupportedChain = (chain: string): chain is SupportedChain => {
  return ["bsc", "ethereum"].includes(chain);
};

export interface AdapterHandler<T, U> {
  (req: T, client?: ClickHouseClient): Promise<U>;
}
