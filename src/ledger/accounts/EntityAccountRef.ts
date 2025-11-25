import { LedgerAccountError } from '@/errors.js';
import { LedgerAccountRef } from '@/ledger/accounts/LedgerAccountRef.js';
import { EntityExternalId } from '@/types.js';

const EXTERNAL_ID_ERROR_MESSAGE =
  'External id must be a finite number or non-empty string without spaces or ":"';

/**
 * Represents a reference to an account associated with an entity. Those accounts
 * are created dynamically and are not preset.
 *
 * Hint: prefix the name with the entity type to avoid name collisions.
 * For example, USER_RECEIVABLES, TEAM_LOCKED_FUNDS, etc.
 */
export class EntityAccountRef extends LedgerAccountRef {
  public readonly type = 'ENTITY' as const;

  public readonly name: string;

  public readonly externalId: EntityExternalId;

  public constructor(
    public readonly ledgerSlug: string,
    name: string,
    externalId: EntityExternalId,
  ) {
    LedgerAccountRef.validateName(name);

    const normalizedExternalId =
      EntityAccountRef.normalizeExternalId(externalId);
    const accountSlug = `${name}:${String(normalizedExternalId)}`;
    super(ledgerSlug, accountSlug);
    this.name = name;
    this.externalId = normalizedExternalId;
  }

  private static normalizeExternalId(
    externalId: EntityExternalId,
  ): EntityExternalId {
    if (typeof externalId === 'number') {
      if (!Number.isFinite(externalId)) {
        throw new LedgerAccountError(EXTERNAL_ID_ERROR_MESSAGE);
      }

      return externalId;
    }

    if (typeof externalId !== 'string') {
      throw new LedgerAccountError(EXTERNAL_ID_ERROR_MESSAGE);
    }

    if (!externalId.trim()) {
      throw new LedgerAccountError(EXTERNAL_ID_ERROR_MESSAGE);
    }

    if (/[:\s]/u.test(externalId)) {
      throw new LedgerAccountError(EXTERNAL_ID_ERROR_MESSAGE);
    }

    return externalId;
  }
}
