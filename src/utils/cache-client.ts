import { Redis } from "ioredis";
import { config } from "dotenv";
config();

const REDIS_URL = process.env.REDIS_URL as string;
const redis = new Redis(REDIS_URL);

export const setResult = async (id: string, res: any) => {
  const key = `result:${id}`;
  const _res = JSON.stringify(res);

  await redis.set(key, _res, "EX", 60 * 60 * 24); // 1 day
  return id;
};

export const getResult = async <T = any>(id: string): Promise<T | undefined> => {
  const key = `result:${id}`;

  const _res = await redis.get(key);
  if (!_res) return;

  const res = JSON.parse(_res) as T;
  return res;
};

export const getRawResult = async (id: string) => {
  const key = `result:${id}`;

  const res = await redis.get(key);
  if (!res) return;
  return res;
};

export const deleteResult = async (id: string) => {
  const key = `result:${id}`;
  await redis.del(key);
};
