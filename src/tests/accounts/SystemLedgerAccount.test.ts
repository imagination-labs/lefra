import { SystemAccountRef } from '@/ledger/accounts/SystemAccountRef.js';
import { randomString } from '#/helpers/chance.js';
import { describe, expect, test } from 'vitest';

const ledgerSlug = randomString();

describe('SystemLedgerAccount', () => {
  test('create system account', () => {
    const account = new SystemAccountRef(ledgerSlug, 'SYSTEM_CURRENT_ASSETS');
    expect(account.accountSlug).toEqual('SYSTEM_CURRENT_ASSETS');
  });

  test('create system account with lowercase name', () => {
    const account = new SystemAccountRef(ledgerSlug, 'system_current_assets');
    expect(account.accountSlug).toEqual('system_current_assets');
  });

  test('create system account with hyphenated name', () => {
    const account = new SystemAccountRef(ledgerSlug, 'SYSTEM-CURRENT-ASSETS');
    expect(account.accountSlug).toEqual('SYSTEM-CURRENT-ASSETS');
  });

  test('cannot create entity account with empty name', () => {
    expect(() => new SystemAccountRef(ledgerSlug, '')).toThrow(
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
      expect(() => new SystemAccountRef(ledgerSlug, name)).toThrow(
        'Account name is invalid. Use letters, digits, underscores, or hyphens, and start/end with a letter or digit.',
      );
    },
  );
});
