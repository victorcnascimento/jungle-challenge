import { MikroORM } from '@mikro-orm/postgresql';
import { createMikroOrmOptions } from './mikro-orm.config';

/**
 * Executa migrations pendentes (up) ou reverte a última migration aplicada (down) via CLI.
 */
async function runCliMigrations(): Promise<void> {
  const direction = process.argv[2] ?? 'up';
  const orm = await MikroORM.init(createMikroOrmOptions());

  try {
    const migrator = orm.getMigrator();
    if (direction === 'down') {
      await migrator.down();
      console.log('Successfully rolled back migration (down).');
    } else {
      await migrator.up();
      console.log('Successfully applied pending migrations (up).');
    }
  } finally {
    await orm.close(true);
  }
}

runCliMigrations().catch((err) => {
  console.error('Migration runner failed:', err);
  process.exit(1);
});
