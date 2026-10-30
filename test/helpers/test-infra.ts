import { createHash, randomUUID } from 'crypto';
import * as http from 'http';
import * as net from 'net';
import { PGlite } from '@electric-sql/pglite';
import { fromNodeSocket } from 'pg-gateway/node';

interface StoredSqsMessage {
  messageId: string;
  body: string;
  groupId: string;
  deduplicationId: string;
  attributes: Record<string, string>;
  messageAttributes: Record<string, unknown>;
  receiveCount: number;
  visibleAt: number;
  receiptHandle?: string;
}

/**
 * Mutex assíncrono simples com suporte a reentrância por proprietário (socket/conexão)
 * para serializar transações SQL (BEGIN ... COMMIT/ROLLBACK) quando múltiplas conexões TCP
 * do pool do MikroORM ou múltiplas instâncias da aplicação compartilham a engine PGlite Postgres 16.
 */
class TransactionAwareMutex {
  private currentOwner: symbol | null = null;
  private holdDepth = 0;
  private queue: Array<{ owner: symbol; resolve: () => void }> = [];

  async acquire(owner: symbol): Promise<void> {
    if (this.currentOwner === owner) {
      this.holdDepth += 1;
      return;
    }
    if (this.currentOwner === null) {
      this.currentOwner = owner;
      this.holdDepth = 1;
      return;
    }
    await new Promise<void>((resolve) => {
      this.queue.push({ owner, resolve });
    });
  }

  release(owner: symbol, forceAll: boolean = false): void {
    if (this.currentOwner !== owner) {
      return;
    }
    if (forceAll) {
      this.holdDepth = 0;
    } else {
      this.holdDepth = Math.max(0, this.holdDepth - 1);
    }

    if (this.holdDepth === 0) {
      const next = this.queue.shift();
      if (next) {
        this.currentOwner = next.owner;
        this.holdDepth = 1;
        next.resolve();
      } else {
        this.currentOwner = null;
      }
    }
  }
}

async function isTcpPortOpen(host: string, port: number): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    const socket = new net.Socket();
    socket.setTimeout(600);
    socket.once('connect', () => {
      socket.destroy();
      resolve(true);
    });
    socket.once('timeout', () => {
      socket.destroy();
      resolve(false);
    });
    socket.once('error', () => {
      socket.destroy();
      resolve(false);
    });
    socket.connect(port, host);
  });
}

function inspectWirePacketSql(data: Uint8Array): {
  sqlStatements: string[];
  hasSimpleQueryOrSync: boolean;
} {
  const sqlStatements: string[] = [];
  let hasSimpleQueryOrSync = false;
  let offset = 0;

  while (offset + 5 <= data.length) {
    const msgType = String.fromCharCode(data[offset]!);
    const view = new DataView(data.buffer, data.byteOffset + offset + 1, 4);
    const msgLen = view.getInt32(0, false);
    if (msgLen < 4 || offset + 1 + msgLen > data.length) {
      break;
    }

    const payloadStart = offset + 5;
    const payloadEnd = offset + 1 + msgLen;

    if (msgType === 'Q') {
      hasSimpleQueryOrSync = true;
      const raw = Buffer.from(data.subarray(payloadStart, payloadEnd - 1)).toString('utf8');
      sqlStatements.push(raw.trim());
    } else if (msgType === 'P') {
      const stmtEnd = data.indexOf(0, payloadStart);
      if (stmtEnd !== -1 && stmtEnd + 1 < payloadEnd) {
        const queryEnd = data.indexOf(0, stmtEnd + 1);
        if (queryEnd !== -1 && queryEnd <= payloadEnd) {
          const raw = Buffer.from(data.subarray(stmtEnd + 1, queryEnd)).toString('utf8');
          sqlStatements.push(raw.trim());
        }
      }
    } else if (msgType === 'S') {
      hasSimpleQueryOrSync = true;
    }

    offset += 1 + msgLen;
  }

  return { sqlStatements, hasSimpleQueryOrSync };
}

/**
 * Inicializa infraestrutura real para os testes de Integração, Concorrência e Carga:
 * - Se os containers Docker (PostgreSQL na porta 5432 e LocalStack na porta 4566) estiverem ativos,
 *   conecta diretamente a eles.
 * - Caso contrário, sobe em processo um servidor TCP PostgreSQL 16 real (via PGlite + pg-gateway)
 *   que executa 100% das migrations, CHECKs, UNIQUEs e Triggers PL/pgSQL, além de um servidor HTTP
 *   compatível com o protocolo AWS SQS JSON 1.0 usado pelo @aws-sdk/client-sqs.
 */
export class TestInfrastructure {
  public databaseUrl!: string;
  public sqsEndpoint!: string;
  public wagerQueueUrl!: string;
  public wagerDlqUrl!: string;
  public eventsQueueUrl!: string;

  private pglite: PGlite | null = null;
  private pgServer: net.Server | null = null;
  private sqsServer: http.Server | null = null;
  private queues = new Map<string, StoredSqsMessage[]>();

  async start(): Promise<void> {
    const externalPgOpen = await isTcpPortOpen('127.0.0.1', 5432);
    const externalSqsOpen = await isTcpPortOpen('127.0.0.1', 4566);

    if (externalPgOpen && process.env.FORCE_EMBEDDED_INFRA !== 'true') {
      this.databaseUrl =
        process.env.DATABASE_URL ??
        'postgresql://jungle:jungle_secret@127.0.0.1:5432/jungle_wagering';
    } else {
      await this.startEmbeddedPostgresServer();
    }

    if (externalSqsOpen && process.env.FORCE_EMBEDDED_INFRA !== 'true') {
      this.sqsEndpoint = process.env.AWS_ENDPOINT ?? 'http://127.0.0.1:4566';
    } else {
      await this.startEmbeddedSqsServer();
    }

    this.wagerQueueUrl = `${this.sqsEndpoint}/000000000000/wager-transactions.fifo`;
    this.wagerDlqUrl = `${this.sqsEndpoint}/000000000000/wager-transactions-dlq.fifo`;
    this.eventsQueueUrl = `${this.sqsEndpoint}/000000000000/domain-integration-events.fifo`;

    process.env.DATABASE_URL = this.databaseUrl;
    process.env.AWS_ENDPOINT = this.sqsEndpoint;
    process.env.AWS_REGION = 'us-east-1';
    process.env.AWS_ACCESS_KEY_ID = 'test';
    process.env.AWS_SECRET_ACCESS_KEY = 'test';
    process.env.SQS_WAGER_QUEUE_URL = this.wagerQueueUrl;
    process.env.SQS_WAGER_DLQ_URL = this.wagerDlqUrl;
    process.env.SQS_EVENTS_QUEUE_URL = this.eventsQueueUrl;
    process.env.DISABLE_BACKGROUND_WORKERS = 'true';
    process.env.LOG_SILENT = process.env.LOG_SILENT ?? 'true';
  }

  async resetState(): Promise<void> {
    this.queues.set('wager-transactions.fifo', []);
    this.queues.set('wager-transactions-dlq.fifo', []);
    this.queues.set('domain-integration-events.fifo', []);
  }

  getQueueMessages(queueName: string): StoredSqsMessage[] {
    return this.queues.get(queueName) ?? [];
  }

  async stop(): Promise<void> {
    if (this.sqsServer) {
      await new Promise<void>((resolve) => this.sqsServer!.close(() => resolve()));
      this.sqsServer = null;
    }
    if (this.pgServer) {
      await new Promise<void>((resolve) => this.pgServer!.close(() => resolve()));
      this.pgServer = null;
    }
    if (this.pglite) {
      await this.pglite.close();
      this.pglite = null;
    }
  }

  private async startEmbeddedPostgresServer(): Promise<void> {
    this.pglite = await PGlite.create();
    const txMutex = new TransactionAwareMutex();

    this.pgServer = net.createServer((socket) => {
      const connId = Symbol('pg-socket');
      let inExplicitTx = false;
      let pendingEndTx = false;

      socket.on('close', () => {
        if (inExplicitTx && this.pglite) {
          inExplicitTx = false;
          pendingEndTx = false;
          this.pglite
            .exec('ROLLBACK;')
            .catch(() => {})
            .finally(() => txMutex.release(connId, true));
        } else {
          txMutex.release(connId, true);
        }
      });

      socket.on('error', () => {
        txMutex.release(connId, true);
      });

      fromNodeSocket(socket, {
        serverVersion: '16.3',
        auth: { method: 'trust' },
        onStartup: async () => {
          await this.pglite!.waitReady;
        },
        onMessage: async (data, { isAuthenticated }) => {
          if (!isAuthenticated) {
            return undefined;
          }

          const { sqlStatements, hasSimpleQueryOrSync } = inspectWirePacketSql(data);

          for (const sql of sqlStatements) {
            if (/^(BEGIN|START\s+TRANSACTION)\b/i.test(sql)) {
              if (!inExplicitTx) {
                await txMutex.acquire(connId);
                inExplicitTx = true;
                pendingEndTx = false;
              }
            }
            if (/^(COMMIT|ROLLBACK)\b/i.test(sql)) {
              if (inExplicitTx) {
                pendingEndTx = true;
              }
            }
          }

          if (!inExplicitTx) {
            await txMutex.acquire(connId);
          }

          try {
            const response = await this.pglite!.execProtocolRaw(data);
            return response;
          } finally {
            if (inExplicitTx && pendingEndTx && hasSimpleQueryOrSync) {
              inExplicitTx = false;
              pendingEndTx = false;
              txMutex.release(connId, true);
            } else if (!inExplicitTx) {
              txMutex.release(connId, false);
            }
          }
        },
      });
    });

    await new Promise<void>((resolve) => {
      this.pgServer!.listen(0, '127.0.0.1', () => resolve());
    });

    const address = this.pgServer.address() as net.AddressInfo;
    this.databaseUrl = `postgresql://postgres:postgres@127.0.0.1:${address.port}/postgres`;
  }

  private async startEmbeddedSqsServer(): Promise<void> {
    await this.resetState();

    this.sqsServer = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on('data', (chunk: Buffer) => chunks.push(chunk));
      req.on('end', () => {
        const rawBody = Buffer.concat(chunks).toString('utf8');
        const target = String(req.headers['x-amz-target'] ?? '');

        try {
          if (target.startsWith('AmazonSQS.')) {
            const action = target.slice('AmazonSQS.'.length);
            const payload = rawBody
              ? (JSON.parse(rawBody) as Record<string, unknown>)
              : {};
            const result = this.handleSqsJsonAction(action, payload);
            res.writeHead(200, {
              'Content-Type': 'application/x-amz-json-1.0',
            });
            res.end(JSON.stringify(result));
            return;
          }

          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({}));
        } catch (err) {
          res.writeHead(400, {
            'Content-Type': 'application/x-amz-json-1.0',
          });
          res.end(
            JSON.stringify({
              __type: 'com.amazonaws.sqs#QueueDoesNotExist',
              message: err instanceof Error ? err.message : String(err),
            }),
          );
        }
      });
    });

    await new Promise<void>((resolve) => {
      this.sqsServer!.listen(0, '127.0.0.1', () => resolve());
    });

    const address = this.sqsServer.address() as net.AddressInfo;
    this.sqsEndpoint = `http://127.0.0.1:${address.port}`;
  }

  private resolveQueueName(queueUrl: string): string {
    const parts = queueUrl.split('/');
    return parts[parts.length - 1] || 'wager-transactions.fifo';
  }

  private handleSqsJsonAction(
    action: string,
    payload: Record<string, unknown>,
  ): Record<string, unknown> {
    const now = Date.now();

    switch (action) {
      case 'ListQueues': {
        return {
          QueueUrls: Array.from(this.queues.keys()).map(
            (name) => `${this.sqsEndpoint}/000000000000/${name}`,
          ),
        };
      }

      case 'CreateQueue': {
        const queueName = String(payload.QueueName ?? 'default.fifo');
        if (!this.queues.has(queueName)) {
          this.queues.set(queueName, []);
        }
        return {
          QueueUrl: `${this.sqsEndpoint}/000000000000/${queueName}`,
        };
      }

      case 'GetQueueAttributes': {
        const queueName = this.resolveQueueName(String(payload.QueueUrl ?? ''));
        const list = this.queues.get(queueName) ?? [];
        const visibleCount = list.filter((m) => m.visibleAt <= now).length;
        return {
          Attributes: {
            ApproximateNumberOfMessages: String(visibleCount),
            QueueArn: `arn:aws:sqs:us-east-1:000000000000:${queueName}`,
          },
        };
      }

      case 'PurgeQueue': {
        const queueName = this.resolveQueueName(String(payload.QueueUrl ?? ''));
        this.queues.set(queueName, []);
        return {};
      }

      case 'SendMessage': {
        const queueName = this.resolveQueueName(String(payload.QueueUrl ?? ''));
        if (!this.queues.has(queueName)) {
          this.queues.set(queueName, []);
        }
        const list = this.queues.get(queueName)!;
        const messageId = randomUUID();
        const dedupId = String(payload.MessageDeduplicationId ?? messageId);
        const groupId = String(payload.MessageGroupId ?? 'default');

        const bodyStr = String(payload.MessageBody ?? '');
        const md5Body = createHash('md5').update(bodyStr, 'utf8').digest('hex');
        const msg: StoredSqsMessage = {
          messageId,
          body: bodyStr,
          groupId,
          deduplicationId: dedupId,
          attributes: {},
          messageAttributes:
            (payload.MessageAttributes as Record<string, unknown>) ?? {},
          receiveCount: 0,
          visibleAt: now,
        };
        list.push(msg);

        return {
          MessageId: messageId,
          MD5OfMessageBody: md5Body,
          SequenceNumber: String(list.length),
        };
      }

      case 'ReceiveMessage': {
        const queueName = this.resolveQueueName(String(payload.QueueUrl ?? ''));
        const list = this.queues.get(queueName) ?? [];
        const maxMessages = Number(payload.MaxNumberOfMessages ?? 10);
        const visibilityTimeoutSec = Number(payload.VisibilityTimeout ?? 30);

        const available: StoredSqsMessage[] = [];
        for (const msg of list) {
          if (msg.visibleAt <= now && available.length < maxMessages) {
            msg.receiveCount += 1;
            msg.visibleAt = now + visibilityTimeoutSec * 1000;
            msg.receiptHandle = `rh-${msg.messageId}-${msg.receiveCount}-${randomUUID()}`;
            available.push(msg);
          }
        }

        return {
          Messages: available.map((m) => ({
            MessageId: m.messageId,
            ReceiptHandle: m.receiptHandle,
            MD5OfBody: createHash('md5').update(m.body, 'utf8').digest('hex'),
            Body: m.body,
            Attributes: {
              ApproximateReceiveCount: String(m.receiveCount),
              MessageGroupId: m.groupId,
              MessageDeduplicationId: m.deduplicationId,
            },
            MessageAttributes: m.messageAttributes,
          })),
        };
      }

      case 'DeleteMessage': {
        const queueName = this.resolveQueueName(String(payload.QueueUrl ?? ''));
        const receiptHandle = String(payload.ReceiptHandle ?? '');
        const list = this.queues.get(queueName) ?? [];
        const idx = list.findIndex((m) => m.receiptHandle === receiptHandle);
        if (idx !== -1) {
          list.splice(idx, 1);
        }
        return {};
      }

      case 'ChangeMessageVisibility': {
        const queueName = this.resolveQueueName(String(payload.QueueUrl ?? ''));
        const receiptHandle = String(payload.ReceiptHandle ?? '');
        const visibilityTimeoutSec = Number(payload.VisibilityTimeout ?? 0);
        const list = this.queues.get(queueName) ?? [];
        const msg = list.find((m) => m.receiptHandle === receiptHandle);
        if (msg) {
          msg.visibleAt = now + visibilityTimeoutSec * 1000;
        }
        return {};
      }

      default:
        return {};
    }
  }
}
