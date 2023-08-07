import _stringify from "json-stable-stringify";

export const formatDate = (date: Date): string => {
  return date.toISOString().replace("T", " ").substring(0, 19);
};

/**
 * Deterministically stringify an object then hashes it with SHA-1.
 *
 * @param params Params to stringify
 * @returns A hash of the params
 */
export const toHash = (params: any, hasher: (input: string, seed?: bigint | undefined) => string): string => {
  const str = _stringify(params, { space: 0 });
  return hasher(str);
};

export const stringify = (params: any): string => {
  return _stringify(params, { space: 0 });
};

export const stablize = (params: any): any => {
  const str = _stringify(params, { space: 0 });
  return JSON.parse(str);
};
