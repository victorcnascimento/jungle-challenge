import { Migration } from '@mikro-orm/migrations';

/**
 * Migration versionada e reversível que aplica todas as garantias de unicidade,
 * imutabilidade e não-negatividade diretamente no schema do PostgreSQL (Seção 5.9).
 */
export class Migration202610270001_InitialSchema extends Migration {
  async up(): Promise<void> {
    // 1. Tabela de Wallets (Aggregate Root)
    this.addSql(`
      CREATE TABLE IF NOT EXISTS wallets (
        id UUID PRIMARY KEY,
        player_id VARCHAR(128) NOT NULL,
        currency CHAR(3) NOT NULL,
        balance_amount NUMERIC(18, 2) NOT NULL,
        version INTEGER NOT NULL DEFAULT 1,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_wallets_player_currency UNIQUE (player_id, currency),
        CONSTRAINT chk_wallets_balance_non_negative CHECK (balance_amount >= 0),
        CONSTRAINT chk_wallets_version_positive CHECK (version >= 1)
      );
    `);

    // 2. Tabela de Transações de Aposta (WagerTransaction)
    this.addSql(`
      CREATE TABLE IF NOT EXISTS wager_transactions (
        id UUID PRIMARY KEY,
        provider_id VARCHAR(128) NOT NULL,
        external_transaction_id VARCHAR(255) NOT NULL,
        idempotency_key VARCHAR(255) NOT NULL,
        payload_hash CHAR(64) NOT NULL,
        wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE RESTRICT,
        player_id VARCHAR(128) NOT NULL,
        round_id VARCHAR(128) NOT NULL,
        game_id VARCHAR(128) NOT NULL,
        kind VARCHAR(32) NOT NULL,
        amount NUMERIC(18, 2) NOT NULL,
        currency CHAR(3) NOT NULL,
        reference_external_transaction_id VARCHAR(255) NULL,
        reference_transaction_id UUID NULL REFERENCES wager_transactions(id) ON DELETE RESTRICT,
        status VARCHAR(32) NOT NULL,
        failure_code VARCHAR(64) NULL,
        observed_balance_amount NUMERIC(18, 2) NULL,
        retry_attempts INTEGER NOT NULL DEFAULT 0,
        next_retry_at TIMESTAMPTZ NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        processed_at TIMESTAMPTZ NULL,
        CONSTRAINT uq_wager_tx_idempotency_key UNIQUE (idempotency_key),
        CONSTRAINT uq_wager_tx_provider_external UNIQUE (provider_id, external_transaction_id),
        CONSTRAINT chk_wager_tx_kind CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
        CONSTRAINT chk_wager_tx_status CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
        CONSTRAINT chk_wager_tx_amount_non_negative CHECK (
          (kind = 'LOSS' AND amount >= 0) OR (kind <> 'LOSS' AND amount > 0)
        ),
        CONSTRAINT chk_wager_tx_observed_balance_non_negative CHECK (
          observed_balance_amount IS NULL OR observed_balance_amount >= 0
        ),
        CONSTRAINT chk_wager_tx_reversal_requires_ref CHECK (
          (kind IN ('REFUND', 'ROLLBACK') AND reference_external_transaction_id IS NOT NULL)
          OR (kind NOT IN ('REFUND', 'ROLLBACK'))
        )
      );
    `);

    // Índice único parcial que impede no banco que uma mesma transação de referência
    // seja revertida mais de uma vez com status PROCESSED (Regra 7.4 e Tabela 7)
    this.addSql(`
      CREATE UNIQUE INDEX IF NOT EXISTS uq_wager_tx_single_processed_reversal
      ON wager_transactions (reference_transaction_id)
      WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');
    `);

    this.addSql(`
      CREATE INDEX IF NOT EXISTS idx_wager_tx_wallet_created
      ON wager_transactions (wallet_id, created_at DESC);
    `);

    this.addSql(`
      CREATE INDEX IF NOT EXISTS idx_wager_tx_pending_reference_due
      ON wager_transactions (next_retry_at ASC, created_at ASC)
      WHERE status = 'PENDING_REFERENCE';
    `);

    // 3. Tabela de Lançamentos Contábeis (WalletLedgerEntry — Estritamente Imutável / Append-Only)
    this.addSql(`
      CREATE TABLE IF NOT EXISTS wallet_ledger_entries (
        id UUID PRIMARY KEY,
        sequence_number BIGSERIAL NOT NULL UNIQUE,
        wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE RESTRICT,
        transaction_id UUID NOT NULL REFERENCES wager_transactions(id) ON DELETE RESTRICT,
        direction VARCHAR(16) NOT NULL,
        amount NUMERIC(18, 2) NOT NULL,
        currency CHAR(3) NOT NULL,
        balance_before NUMERIC(18, 2) NOT NULL,
        balance_after NUMERIC(18, 2) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_ledger_single_entry_per_tx UNIQUE (transaction_id),
        CONSTRAINT chk_ledger_direction CHECK (direction IN ('DEBIT', 'CREDIT')),
        CONSTRAINT chk_ledger_amount_positive CHECK (amount > 0),
        CONSTRAINT chk_ledger_balances_non_negative CHECK (balance_before >= 0 AND balance_after >= 0),
        CONSTRAINT chk_ledger_arithmetic_balanced CHECK (
          (direction = 'CREDIT' AND balance_after = balance_before + amount)
          OR (direction = 'DEBIT' AND balance_after = balance_before - amount)
        )
      );
    `);

    this.addSql(`
      CREATE INDEX IF NOT EXISTS idx_ledger_wallet_sequence
      ON wallet_ledger_entries (wallet_id, sequence_number ASC);
    `);

    // Trigger de Imutabilidade no PostgreSQL: proíbe UPDATE e DELETE em wallet_ledger_entries (Restrição 5 e 9)
    this.addSql(`
      CREATE OR REPLACE FUNCTION prevent_wallet_ledger_mutation()
      RETURNS TRIGGER AS $$
      BEGIN
        RAISE EXCEPTION 'wallet_ledger_entries is immutable (append-only): % operation is forbidden', TG_OP
          USING ERRCODE = 'integrity_constraint_violation';
      END;
      $$ LANGUAGE plpgsql;
    `);

    this.addSql(`
      DROP TRIGGER IF EXISTS trg_prevent_wallet_ledger_mutation ON wallet_ledger_entries;
      CREATE TRIGGER trg_prevent_wallet_ledger_mutation
      BEFORE UPDATE OR DELETE ON wallet_ledger_entries
      FOR EACH ROW EXECUTE FUNCTION prevent_wallet_ledger_mutation();
    `);

    // 4. Tabela de Transactional Inbox (Deduplicação persistente de consumidores SQS)
    this.addSql(`
      CREATE TABLE IF NOT EXISTS inbox_messages (
        consumer_name VARCHAR(128) NOT NULL,
        message_id VARCHAR(255) NOT NULL,
        payload_hash CHAR(64) NOT NULL,
        received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        processed_at TIMESTAMPTZ NULL,
        PRIMARY KEY (consumer_name, message_id)
      );
    `);

    // 5. Tabela de Transactional Outbox (Publicação assíncrona atômica pós-commit)
    this.addSql(`
      CREATE TABLE IF NOT EXISTS outbox_messages (
        id UUID PRIMARY KEY,
        aggregate_id VARCHAR(128) NOT NULL,
        event_type VARCHAR(128) NOT NULL,
        payload JSONB NOT NULL,
        occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        attempts INTEGER NOT NULL DEFAULT 0,
        next_attempt_at TIMESTAMPTZ NULL,
        published_at TIMESTAMPTZ NULL,
        CONSTRAINT chk_outbox_attempts_non_negative CHECK (attempts >= 0)
      );
    `);

    this.addSql(`
      CREATE INDEX IF NOT EXISTS idx_outbox_pending_due
      ON outbox_messages (next_attempt_at ASC NULLS FIRST, occurred_at ASC)
      WHERE published_at IS NULL;
    `);
  }

  async down(): Promise<void> {
    this.addSql('DROP TABLE IF EXISTS outbox_messages CASCADE;');
    this.addSql('DROP TABLE IF EXISTS inbox_messages CASCADE;');
    this.addSql('DROP TRIGGER IF EXISTS trg_prevent_wallet_ledger_mutation ON wallet_ledger_entries;');
    this.addSql('DROP FUNCTION IF EXISTS prevent_wallet_ledger_mutation();');
    this.addSql('DROP TABLE IF EXISTS wallet_ledger_entries CASCADE;');
    this.addSql('DROP TABLE IF EXISTS wager_transactions CASCADE;');
    this.addSql('DROP TABLE IF EXISTS wallets CASCADE;');
  }
}
