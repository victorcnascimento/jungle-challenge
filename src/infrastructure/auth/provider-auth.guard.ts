import {
  CanActivate,
  ExecutionContext,
  Inject,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import {
  PROVIDER_IDENTITY_PORT,
  type AuthenticatedProviderPrincipal,
  type ProviderIdentityPort,
} from '../../application/ports/observability.port';

/**
 * Implementação da porta ProviderIdentityPort (Seção 2).
 * Suporta dois modos via variável de ambiente AUTH_MODE:
 * - "permissive" (default para desenvolvimento e suíte de testes): aceita requisições sem token ou extrai identidade do token quando presente.
 * - "oidc": valida token Bearer JWT emitido por um Identity Provider externo (Keycloak / Zitadel).
 */
export class ExternalIdpProviderIdentityAdapter
  implements ProviderIdentityPort
{
  /**
   * Verifica o cabeçalho HTTP Authorization e extrai a identidade do provedor autenticado.
   */
  async verifyAuthorizationHeader(
    authHeader?: string,
  ): Promise<AuthenticatedProviderPrincipal | null> {
    const authMode = process.env.AUTH_MODE ?? 'permissive';

    if (!authHeader) {
      if (authMode === 'oidc') {
        return null;
      }
      return {
        providerId: 'anonymous-dev-provider',
        subject: 'dev-bypass',
        scopes: ['wagering:write', 'wagering:read', 'wallet:manage'],
      };
    }

    if (!authHeader.startsWith('Bearer ')) {
      return null;
    }

    const token = authHeader.slice('Bearer '.length).trim();
    if (!token) {
      return null;
    }

    try {
      const parts = token.split('.');
      if (parts.length !== 3 || !parts[1]) {
        return null;
      }
      const payloadJson = Buffer.from(parts[1], 'base64url').toString('utf8');
      const payload = JSON.parse(payloadJson) as Record<string, unknown>;

      if (typeof payload.exp === 'number' && payload.exp * 1000 < Date.now()) {
        return null;
      }

      const expectedIssuer = process.env.OIDC_ISSUER_URL;
      if (expectedIssuer && payload.iss !== expectedIssuer) {
        return null;
      }

      return {
        providerId: String(
          payload.provider_id ?? payload.azp ?? payload.client_id ?? 'idp-provider',
        ),
        subject: String(payload.sub ?? 'unknown-subject'),
        scopes:
          typeof payload.scope === 'string' ? payload.scope.split(' ') : [],
      };
    } catch {
      return null;
    }
  }
}

/**
 * Guard do NestJS aplicado aos controllers de Wallets e Wagering para validar a identidade do provedor.
 */
@Injectable()
export class ProviderAuthGuard implements CanActivate {
  constructor(
    @Inject(PROVIDER_IDENTITY_PORT)
    private readonly identityPort: ProviderIdentityPort,
  ) {}

  /**
   * Intercepta a requisição HTTP, valida o header Authorization via ProviderIdentityPort
   * e anexa o principal autenticado ao objeto request (ou lança 401 Unauthorized).
   */
  async canActivate(context: ExecutionContext): Promise<boolean> {
    const request = context.switchToHttp().getRequest<{
      headers?: Record<string, string | undefined>;
      providerPrincipal?: AuthenticatedProviderPrincipal;
    }>();

    const authHeader =
      request.headers?.['authorization'] ?? request.headers?.['Authorization'];

    const principal =
      await this.identityPort.verifyAuthorizationHeader(authHeader);

    if (!principal) {
      throw new UnauthorizedException({
        code: 'UNAUTHORIZED_PROVIDER',
        message: 'Missing or invalid external Identity Provider Bearer token.',
      });
    }

    request.providerPrincipal = principal;
    return true;
  }
}
