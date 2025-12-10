import { EntityAccountRef } from '@/ledger/accounts/EntityAccountRef.js';
import { randomInt, randomString } from '#/helpers/chance.js';
import { describe, expect, test } from 'vitest';

const ledgerSlug = randomString();

describe('EntityLedgerAccount', () => {
  test('create entity entity account', () => {
    const account = new EntityAccountRef(ledgerSlug, 'USER_RECEIVABLES', 1);

    expect(account.externalId).toEqual(1);
    expect(account.name).toEqual('USER_RECEIVABLES');
    expect(account.accountSlug).toEqual('USER_RECEIVABLES:1');
  });

  test('create entity user account', () => {
    const userAccountId = randomInt();
    const account = new EntityAccountRef(
      ledgerSlug,
      'USER_RECEIVABLES_LOCKED',
      userAccountId,
    );

    expect(account.externalId).toEqual(userAccountId);
    expect(account.accountSlug).toEqual(
      `USER_RECEIVABLES_LOCKED:${userAccountId}`,
    );
    expect(account.name).toEqual('USER_RECEIVABLES_LOCKED');
  });

  test('create entity account with lowercase name', () => {
    const account = new EntityAccountRef(ledgerSlug, 'user_receivables', 1);

    expect(account.accountSlug).toEqual('user_receivables:1');
  });

  test('create entity account with string external id', () => {
    const externalId = randomString();
    const account = new EntityAccountRef(
      ledgerSlug,
      'USER_RECEIVABLES',
      externalId,
    );

    expect(account.externalId).toEqual(externalId);
    expect(account.accountSlug).toEqual(`USER_RECEIVABLES:${externalId}`);
  });

  test('create entity account with hyphenated name', () => {
    const account = new EntityAccountRef(
      ledgerSlug,
      'USER-RECEIVABLES',
      randomInt(),
    );

    expect(account.accountSlug.startsWith('USER-RECEIVABLES:')).toBe(true);
  });

  test.each([
    ['empty string', '   '],
    ['contains whitespace', 'user id'],
    ['contains colon', 'id:1'],
  ])(
    'cannot create entity account with invalid external id when %s',
    (_description, externalId) => {
      expect(
        () => new EntityAccountRef(ledgerSlug, 'USER_RECEIVABLES', externalId),
      ).toThrow(
        'External id must be a finite number or non-empty string without spaces or ":"',
      );
    },
  );

  test('cannot create entity account with empty name', () => {
    expect(() => new EntityAccountRef(ledgerSlug, '', 1)).toThrow(
      'Account name cannot be empty',
    );
  });

  test.each([
    ['specialChars!', 'specialChars!'],
    ['QWE_RTY_', 'QWE_RTY_'],
    ['{}', '{}'],
    ['NAME-', 'NAME-'],
    ['-NAME', '-NAME'],
    ['contains space', 'NAME WITH SPACE'],
    ['contains colon', 'NAME:VALUE'],
  ])(
    'cannot create entity account with invalid name %s',
    (_description, name) => {
      expect(() => new EntityAccountRef(ledgerSlug, name, 1)).toThrow(
        'Account name is invalid. Use letters, digits, underscores, or hyphens, and start/end with a letter or digit.',
      );
    },
  );
});
