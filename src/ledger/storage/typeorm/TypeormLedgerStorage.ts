import { LedgerError, LedgerNotFoundError } from '@/errors.js';
import { EntityAccountRef } from '@/ledger/accounts/EntityAccountRef.js';
import { LedgerAccountRef } from '@/ledger/accounts/LedgerAccountRef.js';
import { SystemAccountRef } from '@/ledger/accounts/SystemAccountRef.js';
import { LedgerStorage } from '@/ledger/storage/LedgerStorage.js';
import { Transaction } from '@/ledger/transaction/Transaction.js';
import { Unit, UnitCode } from '@/ledger/units/Unit.js';
import {
  DB_ID,
  InputLedgerAccount,
  InputLedgerAccountType,
  InputLedgerCurrency,
  LedgerInput,
  PersistedEntry,
  PersistedLedger,
  PersistedLedgerAccount,
  PersistedLedgerAccountType,
  PersistedTransaction,
} from '@/types.js';

type QueryRunnerLike = {
  commitTransaction?: () => Promise<void>;
  isTransactionActive?: boolean;
  manager?: EntityManagerLike;
  query: (query: string, parameters?: unknown[]) => Promise<unknown>;
  release?: () => Promise<void>;
  rollbackTransaction?: () => Promise<void>;
  startTransaction?: () => Promise<void>;
};

type TransactionalQueryRunner = QueryRunnerLike & {
  commitTransaction: () => Promise<void>;
  rollbackTransaction: () => Promise<void>;
  startTransaction: () => Promise<void>;
};

type EntityManagerLike = {
  query: (query: string, parameters?: unknown[]) => Promise<unknown>;
  queryRunner?: QueryRunnerLike | null;
};

type Queryable = {
  query: (query: string, parameters?: unknown[]) => Promise<unknown>;
};

const isEntityManagerLike = (
  value: QueryRunnerLike | EntityManagerLike,
): value is EntityManagerLike => {
  return (
    typeof value === 'object' &&
    value !== null &&
    'query' in value &&
    !('startTransaction' in value)
  );
};

const pickField = <T>(
  row: Record<string, unknown>,
  snakeCase: string,
  camelCase: string,
): T => {
  return (row[snakeCase] ?? row[camelCase]) as T;
};

const toDatabaseId = (value: unknown): DB_ID => {
  if (typeof value !== 'number') {
    throw new LedgerError('Unexpected database id shape');
  }

  return value;
};

const isUniqueViolation = (error: unknown, constraint?: string): boolean => {
  const maybeError = error as {
    cause?: { code?: string; constraint?: string };
    code?: string;
    constraint?: string;
    driverError?: { code?: string; constraint?: string };
  };

  const code =
    maybeError.code ?? maybeError.cause?.code ?? maybeError.driverError?.code;
  const errorConstraint =
    maybeError.constraint ??
    maybeError.cause?.constraint ??
    maybeError.driverError?.constraint;

  if (code !== '23505') {
    return false;
  }

  if (constraint && errorConstraint && constraint !== errorConstraint) {
    return false;
  }

  return true;
};

export type TypeormLedgerStorageOptions = {
  /**
   * When true (default), the storage will start/commit/rollback its own
   * transaction unless a transaction is already active on the runner.
   * When false, it assumes the caller manages transactions on the provided
   * QueryRunner/EntityManager.
   */
  manageTransaction?: boolean;
};

/**
 * TypeORM-backed storage that runs on a provided QueryRunner or EntityManager.
 * Use manageTransaction=false when you want Lefra operations to participate in
 * an existing transaction.
 */
export class TypeormLedgerStorage implements LedgerStorage {
  private readonly manageTransaction: boolean;

  private readonly runner: QueryRunnerLike;

  private readonly manager: EntityManagerLike | null;

  public constructor(
    runnerOrManager: QueryRunnerLike | EntityManagerLike,
    options: TypeormLedgerStorageOptions = {},
  ) {
    this.manageTransaction = options.manageTransaction ?? true;

    if (isEntityManagerLike(runnerOrManager)) {
      this.manager = runnerOrManager;
      this.runner = runnerOrManager.queryRunner ?? {
        query: runnerOrManager.query.bind(runnerOrManager),
      };
    } else {
      this.runner = runnerOrManager;
      this.manager = runnerOrManager.manager ?? null;
    }

    if (!this.runner || typeof this.runner.query !== 'function') {
      throw new LedgerError('QueryRunner or EntityManager is required');
    }

    if (this.manageTransaction && !this.getTransactionRunner()) {
      throw new LedgerError(
        'manageTransaction requires a QueryRunner with transaction methods',
      );
    }
  }

  public async assignAccountTypeToLedger({
    accountTypeId,
    ledgerId,
  }: {
    accountTypeId: DB_ID;
    ledgerId: DB_ID;
  }): Promise<void> {
    await this.execute(
      `
      INSERT INTO ledger_ledger_account_type (ledger_id, ledger_account_type_id)
      VALUES ($1, $2)
      `,
      [ledgerId, accountTypeId],
    );
  }

  public async fetchAccountBalance(
    account: LedgerAccountRef,
  ): Promise<Unit<UnitCode>> {
    const id = await this.getLedgerAccountId(account);
    const { amount } = await this.one<{ amount: string | number }>(
      `SELECT calculate_balance_for_ledger_account($1) AS amount`,
      [id],
    );
    const { id: ledgerId } = await this.getLedgerIdBySlug(account.ledgerSlug);
    const currency = await this.getLedgerCurrency(ledgerId);

    return new Unit(
      amount.toString(),
      currency.currencyCode,
      currency.minimumFractionDigits,
    );
  }

  public async findAccount(
    account: LedgerAccountRef,
  ): Promise<PersistedLedgerAccount | null> {
    return await this.maybeOne<PersistedLedgerAccount>(
      `
      SELECT 
        la.id,
        la.ledger_id AS "ledgerId",
        la.ledger_account_type_id AS "ledgerAccountTypeId",
        la.slug,
        la.description
      FROM ledger_account la        
      WHERE
        la.slug = $1
        AND la.ledger_id = (
          SELECT l.id FROM ledger l WHERE l.slug = $2
        )
      `,
      [account.accountSlug, account.ledgerSlug],
    );
  }

  public async findAccountTypeBySlug(
    slug: string,
  ): Promise<PersistedLedgerAccountType | null> {
    const row = await this.maybeOne<Record<string, unknown>>(
      `
      SELECT
        id,
        slug,
        name,
        description,
        normal_balance,
        is_entity_ledger_account,
        parent_ledger_account_type_id
      FROM ledger_account_type
      WHERE slug = $1
      `,
      [slug],
    );

    if (!row) {
      return null;
    }

    return this.mapLedgerAccountType(row);
  }

  public async findEntityAccountTypes(
    ledgerId: DB_ID,
  ): Promise<readonly PersistedLedgerAccountType[]> {
    const rows = await this.any<Record<string, unknown>>(
      `
      SELECT
        lat.id,
        lat.slug,
        lat.name,
        lat.description,
        lat.normal_balance,
        lat.is_entity_ledger_account,
        lat.parent_ledger_account_type_id
      FROM ledger_account_type lat
      INNER JOIN ledger_ledger_account_type llat
        ON lat.id = llat.ledger_account_type_id
      WHERE
        llat.ledger_id = $1
        AND lat.is_entity_ledger_account = true
      `,
      [ledgerId],
    );

    return rows.map((row) => this.mapLedgerAccountType(row));
  }

  public async findSystemAccounts(
    ledgerId: DB_ID,
  ): Promise<readonly PersistedLedgerAccount[]> {
    return await this.any<PersistedLedgerAccount>(
      `
      SELECT
        la.id,
        la.ledger_id AS "ledgerId",
        la.ledger_account_type_id AS "ledgerAccountTypeId",
        la.slug,
        la.description
      FROM ledger_account la
      INNER JOIN ledger_account_type lat ON lat.id = la.ledger_account_type_id
      WHERE
        la.ledger_id = $1
        AND lat.is_entity_ledger_account = false
      `,
      [ledgerId],
    );
  }

  public async getLedgerCurrency(
    ledgerId: DB_ID,
  ): Promise<{ currencyCode: UnitCode; minimumFractionDigits: number }> {
    const row = await this.maybeOne<Record<string, unknown>>(
      `
      SELECT
        lc.code AS currency_code,
        lc.minimum_fraction_digits
      FROM ledger l        
      INNER JOIN ledger_currency lc ON lc.id = l.ledger_currency_id
      WHERE l.id = $1
      `,
      [ledgerId],
    );

    if (!row) {
      throw new LedgerNotFoundError(`Ledger ${ledgerId} is not found`);
    }

    return {
      currencyCode: pickField<UnitCode>(row, 'currency_code', 'currencyCode'),
      minimumFractionDigits: pickField<number>(
        row,
        'minimum_fraction_digits',
        'minimumFractionDigits',
      ),
    };
  }

  public async getLedgerIdBySlug(slug: string): Promise<PersistedLedger> {
    const row = await this.maybeOne<Record<string, unknown>>(
      `
      SELECT 
        l.id,
        l.name,
        l.description,
        l.slug,
        l.ledger_currency_id
      FROM ledger l
      WHERE l.slug = $1
      `,
      [slug],
    );

    if (!row) {
      throw new LedgerNotFoundError(`Ledger ${slug} is not found`);
    }

    return {
      description: String(row.description),
      id: toDatabaseId(row.id),
      ledgerCurrencyId: toDatabaseId(
        pickField<number>(row, 'ledger_currency_id', 'ledgerCurrencyId'),
      ),
      name: String(row.name),
      slug: String(row.slug),
    };
  }

  public async getTransactionById(
    transactionId: DB_ID,
  ): Promise<PersistedTransaction> {
    const row = await this.maybeOne<Record<string, unknown>>(
      `
      SELECT
        id,
        ledger_id,
        posted_at,
        description
      FROM ledger_transaction
      WHERE id = $1
      `,
      [transactionId],
    );

    if (!row) {
      throw new LedgerNotFoundError(`Transaction ${transactionId} not found`);
    }

    return {
      description: row.description as string | null,
      id: toDatabaseId(row.id),
      ledgerId: toDatabaseId(pickField<number>(row, 'ledger_id', 'ledgerId')),
      postedAt: new Date(row.posted_at as string),
    };
  }

  public async getTransactionEntries(
    transactionId: DB_ID,
  ): Promise<readonly PersistedEntry[]> {
    const transaction = await this.getTransactionById(transactionId);
    const rows = await this.any<Record<string, unknown>>(
      `
      SELECT
        lte.id,
        lte.ledger_transaction_id,
        lte.ledger_account_id,
        lte.action,
        lte.amount
      FROM ledger_transaction_entry lte
      WHERE lte.ledger_transaction_id = $1
      `,
      [transactionId],
    );

    const currency = await this.getLedgerCurrency(transaction.ledgerId);

    return rows.map((row) => {
      return {
        action: row.action as 'DEBIT' | 'CREDIT',
        amount: new Unit(
          String(row.amount),
          currency.currencyCode,
          currency.minimumFractionDigits,
        ),
        id: toDatabaseId(row.id),
        ledgerAccountId: toDatabaseId(
          pickField<number>(row, 'ledger_account_id', 'ledgerAccountId'),
        ),
        ledgerTransactionId: toDatabaseId(
          pickField<number>(
            row,
            'ledger_transaction_id',
            'ledgerTransactionId',
          ),
        ),
      };
    });
  }

  public async insertAccount({
    description,
    ledgerAccountTypeId,
    ledgerId,
    slug,
  }: InputLedgerAccount): Promise<PersistedLedgerAccount> {
    const accountType = await this.getSavedAccountTypeById(ledgerAccountTypeId);

    try {
      const { id } = await this.one<{ id: number }>(
        `
        INSERT INTO ledger_account (
          description,
          ledger_account_type_id,
          ledger_id,
          slug
        )
        VALUES ($1, $2, $3, $4)
        RETURNING id
        `,
        [description, accountType.id, ledgerId, slug],
      );

      return {
        description,
        id,
        ledgerAccountTypeId,
        ledgerId,
        slug,
      };
    } catch (error) {
      if (isUniqueViolation(error, 'ledger_account_slug_idx')) {
        throw new LedgerError(`Account ${slug} already exists`);
      }

      throw error;
    }
  }

  public async insertAccountType({
    description,
    isEntityLedgerAccount,
    name,
    normalBalance,
    parentLedgerAccountTypeId = null,
    slug,
  }: InputLedgerAccountType): Promise<PersistedLedgerAccountType> {
    const existingAccount = await this.findAccountTypeBySlug(slug);
    if (existingAccount && !existingAccount.isEntityLedgerAccount) {
      throw new LedgerError(`Account type ${slug} already exists.`);
    }

    if (parentLedgerAccountTypeId) {
      const parentAccount = await this.getSavedAccountTypeById(
        parentLedgerAccountTypeId,
      );
      if (!parentAccount) {
        throw new LedgerError('Parent account type not found');
      }

      if (parentAccount.normalBalance !== normalBalance) {
        throw new LedgerError(
          'Parent account type must have the same normal balance',
        );
      }
    }

    const { id } = await this.one<{ id: number }>(
      `
      INSERT INTO ledger_account_type (
        slug,
        name,
        description,
        normal_balance,
        is_entity_ledger_account,
        parent_ledger_account_type_id
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING id
      `,
      [
        slug,
        name,
        description,
        normalBalance,
        isEntityLedgerAccount,
        parentLedgerAccountTypeId,
      ],
    );

    return {
      description,
      id,
      isEntityLedgerAccount,
      name,
      normalBalance,
      parentLedgerAccountTypeId,
      slug,
    };
  }

  public async insertCurrency(parameters: InputLedgerCurrency) {
    const { code, minimumFractionDigits, symbol } = parameters;

    try {
      const { id } = await this.one<{ id: number }>(
        `
        INSERT INTO ledger_currency (code, minimum_fraction_digits, symbol)
        VALUES ($1, $2, $3)
        RETURNING id
      `,
        [code, minimumFractionDigits, symbol],
      );
      return {
        ...parameters,
        id,
      };
    } catch (error) {
      if (isUniqueViolation(error, 'ledger_currency_code_idx')) {
        throw new LedgerError(`Currency ${code} already exists`);
      }

      throw error;
    }
  }

  public async insertLedger(input: LedgerInput) {
    const { description, ledgerCurrencyId, name, slug } = input;
    const { id } = await this.one<{ id: number }>(
      `
      INSERT INTO ledger (slug, name, description, ledger_currency_id)
      VALUES ($1, $2, $3, $4)
      RETURNING id
      `,
      [slug, name, description, ledgerCurrencyId],
    );
    return {
      ...input,
      id,
    };
  }

  public async insertTransaction(
    ledgerTransaction: Transaction,
  ): Promise<PersistedTransaction> {
    const { id: ledgerId } = await this.getLedgerIdBySlug(
      ledgerTransaction.ledgerSlug,
    );
    const postedAt = ledgerTransaction.postedAt ?? new Date();

    return await this.withTransaction(async () => {
      const { id: ledgerTransactionId } = await this.one<{ id: number }>(
        `
        INSERT INTO ledger_transaction (ledger_id, posted_at, description)
        VALUES ($1, $2, $3)
        RETURNING id
        `,
        [ledgerId, postedAt.toISOString(), ledgerTransaction.description],
      );

      for (const entry of ledgerTransaction.transactionDoubleEntries.flatEntries()) {
        const ledgerAccountId = await this.resolveLedgerAccountId(
          ledgerId,
          entry.account,
        );

        await this.execute(
          `
          INSERT INTO ledger_transaction_entry
            (ledger_transaction_id, ledger_account_id, action, amount)
          VALUES ($1, $2, $3, $4)
          `,
          [
            ledgerTransactionId,
            ledgerAccountId,
            entry.action,
            entry.amount.toFullPrecision(),
          ],
        );
      }

      return {
        description: ledgerTransaction.description,
        id: ledgerTransactionId,
        ledgerId,
        postedAt,
      };
    });
  }

  /**
   * Releases the underlying runner if it exposes release().
   * Useful in tests when using a custom QueryRunner implementation.
   */
  public async release() {
    if (this.runner.release) {
      await this.runner.release();
    } else if (this.manager?.queryRunner?.release) {
      await this.manager.queryRunner.release();
    }
  }

  private async resolveLedgerAccountId(
    ledgerId: DB_ID,
    account: LedgerAccountRef,
  ): Promise<number> {
    if (account instanceof SystemAccountRef) {
      const row = await this.maybeOne<{ id: number }>(
        `SELECT ledger_account_id($1, $2) AS id`,
        [ledgerId, account.accountSlug],
      );
      if (!row?.id) {
        throw new LedgerNotFoundError(
          `Account ${account.accountSlug} not found`,
        );
      }

      return row.id;
    } else if (account instanceof EntityAccountRef) {
      const row = await this.maybeOne<{ id: number }>(
        `SELECT ledger_account_id($1, $2, $3) AS id`,
        [ledgerId, account.name, account.externalId.toString()],
      );
      if (!row?.id) {
        throw new LedgerNotFoundError(
          `Account ${account.accountSlug} not found`,
        );
      }

      return row.id;
    }

    throw new LedgerError('Invalid ledger account input');
  }

  private async getSavedAccountTypeById(id: DB_ID) {
    const row = await this.maybeOne<Record<string, unknown>>(
      `
      SELECT
        id,
        slug,
        name,
        description,
        normal_balance,
        is_entity_ledger_account,
        parent_ledger_account_type_id
      FROM ledger_account_type
      WHERE id = $1
      `,
      [id],
    );

    if (!row) {
      throw new LedgerError(`Account type ID: ${id} not found`);
    }

    return this.mapLedgerAccountType(row);
  }

  private async getLedgerAccountId(account: LedgerAccountRef): Promise<number> {
    const { id: ledgerId } = await this.getLedgerIdBySlug(account.ledgerSlug);
    const row = await this.maybeOne<{ id: number }>(
      `
      SELECT id
      FROM ledger_account
      WHERE slug = $1 AND ledger_id = $2
      `,
      [account.accountSlug, ledgerId],
    );

    if (!row) {
      throw new LedgerNotFoundError(`Account ${account.accountSlug} not found`);
    }

    return row.id;
  }

  private mapLedgerAccountType(
    row: Record<string, unknown>,
  ): PersistedLedgerAccountType {
    const id = pickField<number>(row, 'id', 'id');
    const description = pickField<string | null>(
      row,
      'description',
      'description',
    );

    return {
      description: description ?? '',
      id: toDatabaseId(id),
      isEntityLedgerAccount: pickField<boolean>(
        row,
        'is_entity_ledger_account',
        'isEntityLedgerAccount',
      ),
      name: pickField<string>(row, 'name', 'name'),
      normalBalance: pickField<'CREDIT' | 'DEBIT'>(
        row,
        'normal_balance',
        'normalBalance',
      ),
      parentLedgerAccountTypeId: pickField<DB_ID | null>(
        row,
        'parent_ledger_account_type_id',
        'parentLedgerAccountTypeId',
      ),
      slug: pickField<string>(row, 'slug', 'slug'),
    };
  }

  private getTransactionRunner(): TransactionalQueryRunner | null {
    if (
      this.runner &&
      this.runner.startTransaction &&
      this.runner.commitTransaction &&
      this.runner.rollbackTransaction
    ) {
      return this.runner as TransactionalQueryRunner;
    }

    if (this.manager?.queryRunner) {
      const candidate = this.manager.queryRunner;
      if (
        candidate.startTransaction &&
        candidate.commitTransaction &&
        candidate.rollbackTransaction
      ) {
        return candidate as TransactionalQueryRunner;
      }
    }

    return null;
  }

  private async withTransaction<T>(routine: () => Promise<T>): Promise<T> {
    if (!this.manageTransaction) {
      return routine();
    }

    const runner = this.getTransactionRunner();
    if (!runner) {
      throw new LedgerError(
        'manageTransaction requires a QueryRunner with transaction methods',
      );
    }

    const alreadyActive = Boolean(runner.isTransactionActive);
    if (alreadyActive) {
      return routine();
    }

    await runner.startTransaction();
    try {
      const result = await routine();
      await runner.commitTransaction();
      return result;
    } catch (error) {
      await runner.rollbackTransaction();
      throw error;
    }
  }

  private get queryable(): Queryable {
    if (this.manager) {
      return this.manager;
    }

    return this.runner;
  }

  private async execute(
    query: string,
    parameters: unknown[] = [],
  ): Promise<void> {
    await this.queryable.query(query, parameters);
  }

  private async any<T>(
    query: string,
    parameters: unknown[] = [],
  ): Promise<T[]> {
    const result = await this.queryable.query(query, parameters);
    if (Array.isArray(result)) {
      return result as T[];
    }

    if (
      typeof result === 'object' &&
      result !== null &&
      'rows' in result &&
      Array.isArray((result as { rows: unknown }).rows)
    ) {
      return (result as { rows: T[] }).rows;
    }

    throw new LedgerError('Unexpected query result shape');
  }

  private async maybeOne<T>(
    query: string,
    parameters: unknown[] = [],
  ): Promise<T | null> {
    const rows = await this.any<T>(query, parameters);
    if (rows.length === 0) {
      return null;
    }

    if (rows.length > 1) {
      throw new LedgerError('Expected at most one row');
    }

    return rows[0];
  }

  private async one<T>(query: string, parameters: unknown[] = []): Promise<T> {
    const row = await this.maybeOne<T>(query, parameters);
    if (!row) {
      throw new LedgerError('Expected a result row');
    }

    return row;
  }
}
