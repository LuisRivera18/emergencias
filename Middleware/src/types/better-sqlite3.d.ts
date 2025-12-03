declare module "better-sqlite3" {
  interface RunResult {
    changes: number;
    lastInsertRowid: number | bigint;
  }

  interface Statement {
    run(...params: any[]): RunResult;
    get(...params: any[]): any;
    all(...params: any[]): any[];
  }

  interface Database {
    prepare(sql: string): Statement;
    exec(sql: string): void;
    transaction(fn: (...params: any[]) => any): (...params: any[]) => any;
    close(): void;
  }

  interface DatabaseConstructor {
    new (path: string): Database;
  }

  const BetterSqlite3: DatabaseConstructor;
  export default BetterSqlite3;
}