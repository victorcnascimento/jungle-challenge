import { Migrator } from '@mikro-orm/migrations';
import { defineConfig, Options } from '@mikro-orm/postgresql';
import {
  InboxMessageSchema,
  OutboxMessageSchema,
  WagerTransactionSchema,
  WalletLedgerEntrySchema,
  WalletSchema,
} from './entities/orm-entities';
import { Migration202610270001_InitialSchema } from './migrations/Migration202610270001_InitialSchema';

/**
 * Constrói a configuração do MikroORM para PostgreSQL com suporte a Migrations transacionais.
 */
export function createMikroOrmOptions(databaseUrl?: string): Options {
  const clientUrl =
    databaseUrl ??
    process.env.DATABASE_URL ??
    'postgresql://jungle:jungle_secret@localhost:5432/jungle_wagering';

  return defineConfig({
    clientUrl,
    entities: [
      WalletSchema,
      WagerTransactionSchema,
      WalletLedgerEntrySchema,
      InboxMessageSchema,
      OutboxMessageSchema,
    ],
    extensions: [Migrator],
    migrations: {
      tableName: 'mikro_orm_migrations',
      transactional: true,
      allOrNothing: true,
      migrationsList: [
        {
          name: 'Migration202610270001_InitialSchema',
          class: Migration202610270001_InitialSchema,
        },
      ],
    },
    pool: {
      min: 2,
      max: Number(process.env.DB_POOL_MAX ?? 25),
    },
    allowGlobalContext: true,
  });
}

export default createMikroOrmOptions();
