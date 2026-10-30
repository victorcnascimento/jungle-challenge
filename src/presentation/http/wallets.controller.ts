import {
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  HttpStatus,
  Inject,
  Param,
  Post,
  Query,
  UseGuards,
} from '@nestjs/common';
import { OpenWalletUseCase } from '../../application/use-cases/open-wallet.use-case';
import { ReconcileWalletUseCase } from '../../application/use-cases/reconcile-wallet.use-case';
import { InvalidTransactionContractError } from '../../domain/shared/domain-errors';
import type { MoneyProps } from '../../domain/shared/money.vo';
import { ProviderAuthGuard } from '../../infrastructure/auth/provider-auth.guard';

const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function assertValidUuid(value: string, fieldName: string): void {
  if (!value || !UUID_REGEX.test(value)) {
    throw new InvalidTransactionContractError(
      `Parameter "${fieldName}" must be a valid UUID.`,
    );
  }
}

export class CreateWalletHttpDto {
  playerId!: string;
  initialBalance!: MoneyProps;
}

/**
 * Controller REST para criação, consulta, extrato paginado (Ledger) e reconciliação de Wallets (Seção 9).
 */
@Controller('wallets')
@UseGuards(ProviderAuthGuard)
export class WalletsController {
  constructor(
    @Inject(OpenWalletUseCase)
    private readonly openWalletUseCase: OpenWalletUseCase,
    @Inject(ReconcileWalletUseCase)
    private readonly reconcileWalletUseCase: ReconcileWalletUseCase,
  ) {}

  /**
   * Endpoint POST /wallets: abre uma nova carteira para um jogador e moeda.
   */
  @Post()
  @HttpCode(HttpStatus.CREATED)
  async createWallet(
    @Body() body: CreateWalletHttpDto,
    @Headers('x-correlation-id') correlationId?: string,
  ) {
    if (
      !body ||
      typeof body.playerId !== 'string' ||
      body.playerId.trim().length === 0 ||
      !body.initialBalance
    ) {
      throw new InvalidTransactionContractError(
        'Request body must include non-empty playerId and initialBalance { amount, currency }.',
      );
    }

    return this.openWalletUseCase.execute({
      playerId: body.playerId.trim(),
      initialBalance: body.initialBalance,
      correlationId,
    });
  }

  /**
   * Endpoint GET /wallets/:walletId: retorna o saldo atual e a versão de uma carteira.
   */
  @Get(':walletId')
  async getWallet(@Param('walletId') walletId: string) {
    assertValidUuid(walletId, 'walletId');
    return this.openWalletUseCase.getWalletById(walletId);
  }

  /**
   * Endpoint GET /wallets/:walletId/ledger: retorna os lançamentos contábeis paginados por cursor opaco.
   */
  @Get(':walletId/ledger')
  async getWalletLedger(
    @Param('walletId') walletId: string,
    @Query('cursor') cursor?: string,
    @Query('limit') limitStr?: string,
  ) {
    assertValidUuid(walletId, 'walletId');
    const limit = limitStr ? Number(limitStr) : 50;
    if (Number.isNaN(limit) || limit <= 0) {
      throw new InvalidTransactionContractError(
        'Query parameter "limit" must be a positive integer.',
      );
    }

    const page = await this.openWalletUseCase.getWalletLedger(
      walletId,
      cursor,
      limit,
    );

    return {
      walletId,
      items: page.items.map((entry) => ({
        id: entry.id,
        walletId: entry.walletId,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        createdAt: entry.createdAt.toISOString(),
      })),
      nextCursor: page.nextCursor,
    };
  }

  /**
   * Endpoint POST /wallets/:walletId/reconciliation: executa a auditoria contábil comparando
   * o saldo armazenado na Wallet com o saldo reconstruído pela soma do Ledger.
   */
  @Post(':walletId/reconciliation')
  @HttpCode(HttpStatus.OK)
  async reconcileWallet(
    @Param('walletId') walletId: string,
    @Headers('x-correlation-id') correlationId?: string,
  ) {
    assertValidUuid(walletId, 'walletId');
    return this.reconcileWalletUseCase.execute(walletId, correlationId);
  }
}
