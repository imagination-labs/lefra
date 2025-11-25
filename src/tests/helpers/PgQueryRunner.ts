import { Pool, PoolClient } from 'pg';

/**
 * Minimal QueryRunner-like wrapper backed by pg for integration tests.
 */
export class PgQueryRunner {
  private readonly pool: Pool;

  private client: PoolClient | null = null;

  public isTransactionActive = false;

  public constructor(connectionString: string) {
    this.pool = new Pool({
      connectionString,
    });
  }

  private async getClient(): Promise<PoolClient> {
    if (this.client) {
      return this.client;
    }

    this.client = await this.pool.connect();
    return this.client;
  }

  public async query(query: string, parameters: unknown[] = []) {
    const client = await this.getClient();
    const result = await client.query(query, parameters as unknown[]);
    return result.rows;
  }

  public async startTransaction(): Promise<void> {
    const client = await this.getClient();
    await client.query('BEGIN');
    this.isTransactionActive = true;
  }

  public async commitTransaction(): Promise<void> {
    if (!this.client) {
      return;
    }

    await this.client.query('COMMIT');
    this.isTransactionActive = false;
    await this.release();
  }

  public async rollbackTransaction(): Promise<void> {
    if (!this.client) {
      return;
    }

    await this.client.query('ROLLBACK');
    this.isTransactionActive = false;
    await this.release();
  }

  public async release(): Promise<void> {
    if (this.client) {
      this.client.release();
      this.client = null;
    }

    await this.pool.end();
  }
}
