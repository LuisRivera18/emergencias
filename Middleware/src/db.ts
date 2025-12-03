import Database from "better-sqlite3";
import fs from "fs";
import path from "path";

export class DB {
  db: InstanceType<typeof Database>;

  constructor(dbPath: string, schemaSql: string) {
    const needInit = !fs.existsSync(dbPath);
    this.db = new Database(dbPath);
    if (needInit) {
      const sql = fs.readFileSync(schemaSql, "utf8");
      this.db.exec(sql);
    }
  }

  // Helpers
  run(sql: string, params: any[] = []) {
    const st = this.db.prepare(sql);
    const info = st.run(...params);
    return info;
  }

  get(sql: string, params: any[] = []) {
    return this.db.prepare(sql).get(...params);
  }

  all(sql: string, params: any[] = []) {
    return this.db.prepare(sql).all(...params);
  }

  transaction(fn: (...args: any[]) => any) {
    return this.db.transaction(fn)();
  }
}


