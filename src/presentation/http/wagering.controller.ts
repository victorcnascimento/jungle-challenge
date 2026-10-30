import {
  Body,
  Controller,
  Get,
  Headers,
  HttpStatus,
  Inject,
  Param,
  Post,
  Res,
  UseGuards,
} from '@nestjs/common';
import { ProcessWagerTransactionUseCase } from '../../application/use-cases/process-wager-transaction.use-case';
import { InvalidTransactionContractError } from '../../domain/shared/domain-errors';
import type { MoneyProps } from '../../domain/shared/money.vo';
import {
  WagerTransaction,
  WagerTransactionKind,
  WagerTransactionStatus,
} from '../../domain/wagering/wager-transaction.entity';
import { ProviderAuthGuard } from '../../infrastructure/auth/provider-auth.guard';

const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function assertValidUuid(value: string, fieldName: string): void {
  if (!value || !UUID_REGEX.test(value)) {
    throw new InvalidTransactionContractError(
      `Field "${fieldName}" must be a valid UUID.`,
    );
  }
}

export class SubmitWagerTransactionHttpDto {
  providerId!: string;
  externalTransactionId!: string;
  playerId!: string;
  walletId!: string;
  roundId!: string;
  gameId!: string;
  kind!: WagerTransactionKind;
  money!: MoneyProps;
  referenceExternalTransactionId?: string;
}

/**
 * Controller REST para submissão idempotente e consulta de transações de aposta (Seção 9).
 */
@Controller()
@UseGuards(ProviderAuthGuard)
export class WageringController {
  constructor(
    @Inject(ProcessWagerTransactionUseCase)
    private readonly processWagerUseCase: ProcessWagerTransactionUseCase,
  ) {}

  /**
   * Endpoint POST /wagering/transactions: processa uma transação de aposta exigindo o cabeçalho Idempotency-Key.
   */
  @Post('wagering/transactions')
  async submitTransaction(
    @Headers('idempotency-key') idempotencyKeyHeader: string | undefined,
    @Headers('x-correlation-id') correlationId: string | undefined,
    @Body() body: SubmitWagerTransactionHttpDto,
    @Res()
    res: {
      status: (code: number) => { json: (payload: unknown) => void };
    },
  ) {
    if (!idempotencyKeyHeader || idempotencyKeyHeader.trim().length === 0) {
      throw new InvalidTransactionContractError(
        'Header "Idempotency-Key" is required and is the source of truth for idempotency.',
      );
    }

    if (
      !body ||
      !body.providerId ||
      !body.externalTransactionId ||
      !body.playerId ||
      !body.walletId ||
      !body.roundId ||
      !body.gameId ||
      !body.kind ||
      !body.money
    ) {
      throw new InvalidTransactionContractError(
        'Missing required fields in transaction payload.',
      );
    }

    const allowedKinds = new Set<string>(Object.values(WagerTransactionKind));
    if (!allowedKinds.has(body.kind)) {
      throw new InvalidTransactionContractError(
        `Invalid transaction kind "${body.kind}".`,
      );
    }

    assertValidUuid(body.walletId, 'walletId');

    const result = await this.processWagerUseCase.execute({
      idempotencyKey: idempotencyKeyHeader.trim(),
      providerId: body.providerId,
      externalTransactionId: body.externalTransactionId,
      playerId: body.playerId,
      walletId: body.walletId,
      roundId: body.roundId,
      gameId: body.gameId,
      kind: body.kind,
      money: body.money,
      referenceExternalTransactionId: body.referenceExternalTransactionId,
      correlationId: correlationId ?? idempotencyKeyHeader.trim(),
      channel: 'http',
    });

    // Mapeamento semântico de Status HTTP (Seção 9):
    // - PROCESSED -> 200 OK
    // - PENDING_REFERENCE -> 202 Accepted (aceite com processamento pendente da referência)
    // - REJECTED -> 422 Unprocessable Entity (rejeição por regra de negócio, preservando o corpo estruturado)
    let httpStatus = HttpStatus.OK;
    if (result.status === WagerTransactionStatus.PendingReference) {
      httpStatus = HttpStatus.ACCEPTED;
    } else if (result.status === WagerTransactionStatus.Rejected) {
      httpStatus = HttpStatus.UNPROCESSABLE_ENTITY;
    }

    res.status(httpStatus).json(result);
  }

  /**
   * Endpoint GET /wagering/transactions/:transactionId: consulta detalhes de uma transação pelo seu ID interno.
   */
  @Get('wagering/transactions/:transactionId')
  async getById(@Param('transactionId') transactionId: string) {
    assertValidUuid(transactionId, 'transactionId');
    const tx = await this.processWagerUseCase.getTransactionById(transactionId);
    return this.serializeTransaction(tx);
  }

  /**
   * Endpoint GET /providers/:providerId/wagering/transactions/:externalTransactionId:
   * consulta detalhes de uma transação pelo ID externo enviado pelo provedor de jogos.
   */
  @Get('providers/:providerId/wagering/transactions/:externalTransactionId')
  async getByProviderAndExternalId(
    @Param('providerId') providerId: string,
    @Param('externalTransactionId') externalTransactionId: string,
  ) {
    const tx =
      await this.processWagerUseCase.getTransactionByProviderAndExternalId(
        providerId,
        externalTransactionId,
      );
    return this.serializeTransaction(tx);
  }

  /**
   * Formata a entidade de domínio WagerTransaction para o contrato de resposta JSON da API.
   */
  private serializeTransaction(tx: WagerTransaction) {
    return {
      id: tx.id,
      providerId: tx.providerId,
      externalTransactionId: tx.externalTransactionId,
      idempotencyKey: tx.idempotencyKey,
      walletId: tx.walletId,
      playerId: tx.playerId,
      roundId: tx.roundId,
      gameId: tx.gameId,
      kind: tx.kind,
      money: tx.money.toJSON(),
      status: tx.status,
      ...(tx.referenceExternalTransactionId
        ? { referenceExternalTransactionId: tx.referenceExternalTransactionId }
        : {}),
      ...(tx.referenceTransactionId
        ? { referenceTransactionId: tx.referenceTransactionId }
        : {}),
      ...(tx.failureCode ? { failureCode: tx.failureCode } : {}),
      ...(tx.observedBalance
        ? { observedBalance: tx.observedBalance.toJSON() }
        : {}),
      createdAt: tx.createdAt.toISOString(),
      ...(tx.processedAt ? { processedAt: tx.processedAt.toISOString() } : {}),
    };
  }
}
