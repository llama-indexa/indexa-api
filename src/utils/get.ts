export const get = async (args: { url: string; options?: RequestInit }, timeout: number) => {
  if (!timeout) {
    return await fetch(args.url, args.options);
  }

  const { url, options } = args;
  const c = new AbortController();
  const id = setTimeout(() => c.abort(), timeout);
  const r = await fetch(url, { ...options, signal: c.signal });
  clearTimeout(id);
  return r;
};
