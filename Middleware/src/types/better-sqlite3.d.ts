declare module "better-sqlite3" {
  class Database {
    constructor(path: string);

    prepare(sql: string): {
      run(...params: any[]): { changes: number; lastInsertRowid: number | bigint };
      get(...params: any[]): any;
      all(...params: any[]): any[];
    };

    exec(sql: string): void;

    transaction(fn: (...args: any[]) => any): (...args: any[]) => any;

    close(): void;
  }

  export default Database;
}
