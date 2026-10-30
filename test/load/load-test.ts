import { randomUUID } from 'crypto';
import * as os from 'os';
import { MikroORM } from '@mikro-orm/postgresql';
import { PublishOutboxMessagesUseCase } from '../../src/application/use-cases/publish-outbox.use-case';
import { ReconcileWalletUseCase } from '../../src/application/use-cases/reconcile-wallet.use-case';
import { createMikroOrmOptions } from '../../src/infrastructure/database/mikro-orm.config';
import { createConfiguredApp } from '../../src/main';
import { TestInfrastructure } from '../helpers/test-infra';

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil((p / 100) * sorted.length) - 1),
  );
  return sorted[idx]!;
}

async function runLoadTest(): Promise<void> {
  console.log('================================================================');
  console.log('  JUNGLE GAMING — DISTRIBUTED WAGERING PROCESSOR LOAD TEST');
  console.log('================================================================\n');

  const infra = new TestInfrastructure();
  await infra.start();

  const orm1 = await MikroORM.init(createMikroOrmOptions(infra.databaseUrl));
  await orm1.getMigrator().up();
  const orm2 = await MikroORM.init(createMikroOrmOptions(infra.databaseUrl));
  const orm3 = await MikroORM.init(createMikroOrmOptions(infra.databaseUrl));

  const app1 = await createConfiguredApp({ orm: orm1 });
  const app2 = await createConfiguredApp({ orm: orm2 });
  const app3 = await createConfiguredApp({ orm: orm3 });

  await Promise.all([
    app1.listen(0, '127.0.0.1'),
    app2.listen(0, '127.0.0.1'),
    app3.listen(0, '127.0.0.1'),
  ]);

  const urls = [
    `http://127.0.0.1:${app1.getHttpServer().address().port}`,
    `http://127.0.0.1:${app2.getHttpServer().address().port}`,
    `http://127.0.0.1:${app3.getHttpServer().address().port}`,
  ];

  const allWalletIds: string[] = [];
  const latenciesMs: number[] = [];
  let processedCount = 0;
  let rejectedBusinessCount = 0;
  let idempotentReplayCount = 0;
  let unexpectedErrorCount = 0;

  try {
    // 1. Setup: Criar 20 Wallets para Fase A + 1 Hot Wallet para Fase B
    const multiWallets: Array<{ id: string; playerId: string }> = [];
    for (let i = 0; i < 20; i++) {
      const playerId = randomUUID();
      const res = await fetch(`${urls[i % 3]}/wallets`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          playerId,
          initialBalance: { amount: '500.00', currency: 'BRL' },
        }),
      });
      const w = (await res.json()) as { id: string; playerId: string };
      multiWallets.push(w);
      allWalletIds.push(w.id);
    }

    const hotWalletRes = await fetch(`${urls[0]}/wallets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        playerId: randomUUID(),
        initialBalance: { amount: '1000.00', currency: 'BRL' },
      }),
    });
    const hotWallet = (await hotWalletRes.json()) as {
      id: string;
      playerId: string;
    };
    allWalletIds.push(hotWallet.id);

    const suiteStart = performance.now();

    // -------------------------------------------------------------------------
    // Fase A: Carga distribuída em 20 Wallets (200 requisições em ondas de 25)
    // -------------------------------------------------------------------------
    const phaseARequests: Array<() => Promise<void>> = [];

    for (let i = 0; i < 160; i++) {
      const w = multiWallets[i % multiWallets.length]!;
      const instanceUrl = urls[i % 3]!;
      const extId = `load-a-${i}-${randomUUID()}`;
      const kind = i % 5 === 0 ? 'WIN' : i % 7 === 0 ? 'LOSS' : 'BET';
      const amount = kind === 'LOSS' ? '0.00' : '25.00';

      phaseARequests.push(async () => {
        const t0 = performance.now();
        const res = await fetch(`${instanceUrl}/wagering/transactions`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Idempotency-Key': `provider-load:${extId}`,
          },
          body: JSON.stringify({
            providerId: 'provider-load',
            externalTransactionId: extId,
            playerId: w.playerId,
            walletId: w.id,
            roundId: `round-load-${i}`,
            gameId: 'fortune-chimp',
            kind,
            money: { amount, currency: 'BRL' },
          }),
        });
        latenciesMs.push(performance.now() - t0);

        if (res.status === 200 || res.status === 422) {
          const body = (await res.json()) as {
            status: string;
            idempotentReplay: boolean;
          };
          if (body.idempotentReplay) idempotentReplayCount++;
          else if (body.status === 'PROCESSED') processedCount++;
          else if (body.status === 'REJECTED') rejectedBusinessCount++;
        } else {
          unexpectedErrorCount++;
        }
      });
    }

    // 40 replays idempotentes concorrentes na Fase A
    const sharedReplayExt = `load-replay-${randomUUID()}`;
    const targetReplayWallet = multiWallets[0]!;
    for (let r = 0; r < 40; r++) {
      const instanceUrl = urls[r % 3]!;
      phaseARequests.push(async () => {
        const t0 = performance.now();
        const res = await fetch(`${instanceUrl}/wagering/transactions`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Idempotency-Key': `provider-load:${sharedReplayExt}`,
          },
          body: JSON.stringify({
            providerId: 'provider-load',
            externalTransactionId: sharedReplayExt,
            playerId: targetReplayWallet.playerId,
            walletId: targetReplayWallet.id,
            roundId: 'round-replay-shared',
            gameId: 'fortune-chimp',
            kind: 'BET',
            money: { amount: '10.00', currency: 'BRL' },
          }),
        });
        latenciesMs.push(performance.now() - t0);

        if (res.status === 200) {
          const body = (await res.json()) as { idempotentReplay: boolean };
          if (body.idempotentReplay) idempotentReplayCount++;
          else processedCount++;
        } else {
          unexpectedErrorCount++;
        }
      });
    }

    const waveSize = 25;
    for (let i = 0; i < phaseARequests.length; i += waveSize) {
      const batch = phaseARequests.slice(i, i + waveSize);
      await Promise.all(batch.map((fn) => fn()));
    }

    // -------------------------------------------------------------------------
    // Fase B: Hot-Wallet Contention (100 apostas de 20.00 BRL disputando 1000.00 BRL)
    // Esperado: exatamente 50 PROCESSED e 50 REJECTED (INSUFFICIENT_FUNDS), saldo = 0.00
    // -------------------------------------------------------------------------
    const hotPromises = Array.from({ length: 100 }, async (_, idx) => {
      const instanceUrl = urls[idx % 3]!;
      const extId = `hot-load-${idx}-${randomUUID()}`;
      const t0 = performance.now();
      const res = await fetch(`${instanceUrl}/wagering/transactions`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': `provider-hot:${extId}`,
        },
        body: JSON.stringify({
          providerId: 'provider-hot',
          externalTransactionId: extId,
          playerId: hotWallet.playerId,
          walletId: hotWallet.id,
          roundId: `round-hot-${idx}`,
          gameId: 'fortune-chimp',
          kind: 'BET',
          money: { amount: '20.00', currency: 'BRL' },
        }),
      });
      latenciesMs.push(performance.now() - t0);

      if (res.status === 200 || res.status === 422) {
        const body = (await res.json()) as { status: string };
        if (body.status === 'PROCESSED') processedCount++;
        else if (body.status === 'REJECTED') rejectedBusinessCount++;
      } else {
        unexpectedErrorCount++;
      }
    });

    await Promise.all(hotPromises);

    const totalDurationSec = (performance.now() - suiteStart) / 1000;

    // Medir Outbox Lag antes e depois da drenagem concorrente pelos 3 workers
    const lagBeforeRows = await orm1.em.getConnection().execute<
      Array<{ pending_count: string; max_lag_ms: string }>
    >(
      `SELECT COUNT(*)::text AS pending_count,
              COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(occurred_at))) * 1000, 0)::text AS max_lag_ms
       FROM outbox_messages
       WHERE published_at IS NULL`,
    );
    const outboxPendingBeforeDrain = Number(
      lagBeforeRows[0]?.pending_count ?? 0,
    );
    const outboxMaxLagMsBeforeDrain = Number(
      lagBeforeRows[0]?.max_lag_ms ?? 0,
    );

    const outboxStart = performance.now();
    const pub1 = app1.get(PublishOutboxMessagesUseCase);
    const pub2 = app2.get(PublishOutboxMessagesUseCase);
    const pub3 = app3.get(PublishOutboxMessagesUseCase);

    let totalOutboxPublished = 0;
    while (true) {
      const [b1, b2, b3] = await Promise.all([
        pub1.executeBatch(100),
        pub2.executeBatch(100),
        pub3.executeBatch(100),
      ]);
      const sum = b1 + b2 + b3;
      totalOutboxPublished += sum;
      if (sum === 0) break;
    }
    const outboxDrainDurationMs = performance.now() - outboxStart;

    // Verificar reconciliação de TODAS as 21 carteiras
    const reconciler = app1.get(ReconcileWalletUseCase);
    let consistentWallets = 0;
    let totalLedgerEntries = 0;
    for (const walletId of allWalletIds) {
      const report = await reconciler.execute(walletId);
      if (report.consistent && report.difference.amount === '0.00') {
        consistentWallets += 1;
      }
      totalLedgerEntries += report.checkedEntries;
    }

    latenciesMs.sort((a, b) => a - b);
    const totalRequests = latenciesMs.length;
    const throughputRps = totalRequests / totalDurationSec;
    const meanMs =
      latenciesMs.reduce((acc, v) => acc + v, 0) /
      Math.max(1, latenciesMs.length);

    const summary = {
      environment: {
        os: `${os.platform()} ${os.release()} (${os.arch()})`,
        cpu: os.cpus()[0]?.model ?? 'Unknown CPU',
        cpuCores: os.cpus().length,
        memoryGb: Number((os.totalmem() / 1024 ** 3).toFixed(1)),
        bunVersion: Bun.version,
        instances: 3,
      },
      workload: {
        totalTransactionsSubmitted: totalRequests,
        walletsTested: allWalletIds.length,
        phaseA_multiWalletRequests: 200,
        phaseB_hotWalletContentionRequests: 100,
      },
      results: {
        durationSeconds: Number(totalDurationSec.toFixed(3)),
        throughputRps: Number(throughputRps.toFixed(2)),
        latencyMs: {
          min: Number((latenciesMs[0] ?? 0).toFixed(2)),
          p50: Number(percentile(latenciesMs, 50).toFixed(2)),
          p95: Number(percentile(latenciesMs, 95).toFixed(2)),
          p99: Number(percentile(latenciesMs, 99).toFixed(2)),
          max: Number((latenciesMs[latenciesMs.length - 1] ?? 0).toFixed(2)),
          mean: Number(meanMs.toFixed(2)),
        },
        outcomes: {
          processed: processedCount,
          rejectedByBusinessRule: rejectedBusinessCount,
          idempotentReplays: idempotentReplayCount,
          unexpectedHttpErrors: unexpectedErrorCount,
          errorRatePercent: Number(
            ((unexpectedErrorCount / totalRequests) * 100).toFixed(2),
          ),
        },
        outbox: {
          eventsQueuedDuringRun: outboxPendingBeforeDrain,
          maxLagMsBeforeDrain: Number(outboxMaxLagMsBeforeDrain.toFixed(2)),
          eventsPublishedBy3Workers: totalOutboxPublished,
          drainDurationMs: Number(outboxDrainDurationMs.toFixed(2)),
        },
        invariantVerification: {
          walletsReconciled: `${consistentWallets}/${allWalletIds.length}`,
          allConsistent: consistentWallets === allWalletIds.length,
          totalLedgerEntriesVerified: totalLedgerEntries,
        },
      },
    };

    console.log(JSON.stringify(summary, null, 2));

    if (
      unexpectedErrorCount > 0 ||
      consistentWallets !== allWalletIds.length
    ) {
      process.exitCode = 1;
    }
  } finally {
    await Promise.allSettled([app1.close(), app2.close(), app3.close()]);
    await Promise.allSettled([
      orm1.close(true),
      orm2.close(true),
      orm3.close(true),
    ]);
    await infra.stop();
  }
}

runLoadTest().catch((err) => {
  console.error('Load test failed:', err);
  process.exit(1);
});
