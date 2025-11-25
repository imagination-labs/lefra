import { LedgerAccountError } from '@/errors.js';

type LedgerAccountRefType = 'SYSTEM' | 'ENTITY';
export const ACCOUNT_NAME_SEPARATOR = '_';

// Allow letters, digits, and underscores; must start/end with a letter or digit.
const VALID_NAME_REGEX = /^[\p{L}\d]\w*[\p{L}\d]$/u;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type LedgerAccountRefBuilder = (...args: any) => LedgerAccountRef;

/**
 * Represents a reference to a ledger account.
 * The reference is a slug that is used to identify the account.
 * The reference does not check if the account exists. It is only a reference.
 */
export abstract class LedgerAccountRef {
  public abstract readonly type: LedgerAccountRefType;

  protected constructor(
    public readonly ledgerSlug: string,
    public readonly accountSlug: string,
  ) {}

  protected static validateName(name: string) {
    if (!name) {
      throw new LedgerAccountError('Account name cannot be empty');
    }

    if (!VALID_NAME_REGEX.test(name)) {
      throw new LedgerAccountError(
        `Account name is invalid. Use letters, digits, or underscores, and start/end with a letter or digit. Name: ${name}`,
      );
    }
  }
}
