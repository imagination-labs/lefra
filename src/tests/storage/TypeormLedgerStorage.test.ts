import { LedgerNotFoundError } from '@/errors.js';
import { EntityAccountRef } from '@/ledger/accounts/EntityAccountRef.js';
import { SystemAccountRef } from '@/ledger/accounts/SystemAccountRef.js';
import { TypeormLedgerStorage } from '@/ledger/storage/typeorm/TypeormLedgerStorage.js';
import { doubleEntry } from '@/ledger/transaction/DoubleEntry.js';
import { credit, debit } from '@/ledger/transaction/Entry.js';
import { Transaction } from '@/ledger/transaction/Transaction.js';
import { TransactionDoubleEntries } from '@/ledger/transaction/TransactionDoubleEntries.js';
import { UnitCode } from '@/ledger/units/Unit.js';
import { runWithDatabaseConnectionPool } from '#/helpers/createTestConnection.js';
import { PgQueryRunner } from '#/helpers/PgQueryRunner.js';
import { usd } from '#/helpers/units.js';
import { describe, expect, test } from 'vitest';

const ledgerSlug = 'TYPEORM_LEDGER';

const seedLedger = async (storage: TypeormLedgerStorage) => {
  const currency = await storage.insertCurrency({
    code: 'USD',
    minimumFractionDigits: 2,
    symbol: '$',
  });

  const ledger = await storage.insertLedger({
    description: 'TypeORM test ledger',
    ledgerCurrencyId: currency.id,
    name: 'TypeORM Ledger',
    slug: ledgerSlug,
  });

  const systemIncome = await storage.insertAccountType({
    description: 'System income',
    isEntityLedgerAccount: false,
    name: 'SYSTEM_INCOME',
    normalBalance: 'CREDIT',
    parentLedgerAccountTypeId: null,
    slug: 'SYSTEM_INCOME',
  });
  await storage.assignAccountTypeToLedger({
    accountTypeId: systemIncome.id,
    ledgerId: ledger.id,
  });
  const receivables = await storage.insertAccountType({
    description: 'Receivables',
    isEntityLedgerAccount: true,
    name: 'USER_RECEIVABLES',
    normalBalance: 'DEBIT',
    parentLedgerAccountTypeId: null,
    slug: 'USER_RECEIVABLES',
  });
  await storage.assignAccountTypeToLedger({
    accountTypeId: receivables.id,
    ledgerId: ledger.id,
  });

  const incomePaidProjects = await storage.insertAccount({
    description: 'Income from paid projects',
    ledgerAccountTypeId: systemIncome.id,
    ledgerId: ledger.id,
    slug: 'SYSTEM_INCOME_PAID_PROJECTS',
  });
  const incomePaymentFee = await storage.insertAccount({
    description: 'Income from payment fees',
    ledgerAccountTypeId: systemIncome.id,
    ledgerId: ledger.id,
    slug: 'SYSTEM_INCOME_PAYMENT_FEE',
  });

  return {
    currency,
    incomePaidProjects,
    incomePaymentFee,
    ledger,
    receivables,
  };
};

const createPaymentTransaction = (externalId: number) => {
  const entries = TransactionDoubleEntries.empty<UnitCode>().push(
    doubleEntry(
      [
        debit(
          new EntityAccountRef(ledgerSlug, 'USER_RECEIVABLES', externalId),
          usd(100),
        ),
      ],
      [
        credit(
          new SystemAccountRef(ledgerSlug, 'SYSTEM_INCOME_PAID_PROJECTS'),
          usd(100),
        ),
      ],
    ),
  );

  return new Transaction(entries, 'User receivable collected');
};

describe('TypeormLedgerStorage', () => {
  test('participates in caller-managed transactions via QueryRunner', async () => {
    await runWithDatabaseConnectionPool(async ({ testDatabase }) => {
      const runner = new PgQueryRunner(testDatabase.getConnectionUri());
      const storage = new TypeormLedgerStorage(runner);
      await seedLedger(storage);

      await runner.startTransaction();
      const transaction = await storage.insertTransaction(
        createPaymentTransaction(1),
      );
      // Roll back to make sure storage is not auto-committing.
      await runner.rollbackTransaction();
      await storage.release();

      const verifyRunner = new PgQueryRunner(testDatabase.getConnectionUri());
      const verifyStorage = new TypeormLedgerStorage(verifyRunner);

      await expect(
        verifyStorage.getTransactionById(transaction.id),
      ).rejects.toThrow(LedgerNotFoundError);
      await verifyRunner.release();
    });
  });

  test('can manage its own transactions when requested', async () => {
    await runWithDatabaseConnectionPool(async ({ testDatabase }) => {
      const runner = new PgQueryRunner(testDatabase.getConnectionUri());
      const storage = new TypeormLedgerStorage(runner, {
        manageTransaction: true,
      });
      await seedLedger(storage);

      const transaction = await storage.insertTransaction(
        createPaymentTransaction(2),
      );

      const receivablesBalance = await storage.fetchAccountBalance(
        new EntityAccountRef(ledgerSlug, 'USER_RECEIVABLES', 2),
      );
      expect(receivablesBalance.toString()).toBe('100.00');

      const savedTransaction = await storage.getTransactionById(transaction.id);
      expect(savedTransaction.id).toBe(transaction.id);

      await storage.release();
    });
  });
});
