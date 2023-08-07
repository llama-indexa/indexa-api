// to help with highlighting
export const sql = (strings: TemplateStringsArray, ...values: any[]) => {
  let query = "";
  for (let i = 0; i < strings.length; i++) {
    query += strings[i];
    if (i < values.length) {
      query += values[i];
    }
  }
  return query;
};
