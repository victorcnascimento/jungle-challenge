import Decimal from 'decimal.js';
import { CurrencyMismatchError, InvalidMoneyError } from './domain-errors';

/**
 * DTO de entrada/saída para serialização de valores monetários nos contratos HTTP, SQS e Outbox.
 */
export interface MoneyProps {
  amount: string;   // decimal string, ex.: "25.00"
  currency: string; // ISO-4217, ex.: "BRL"
}

/**
 * Regex estrita para contratos de entrada:
 * - Não aceita sinal (+/-), notação científica (1e5), espaços, NaN ou Infinity.
 * - Exige parte inteira sem zeros à esquerda desnecessários (exceto "0.xx") e escala fixa de exatamente 2 casas decimais.
 */
const STRICT_NON_NEGATIVE_AMOUNT_REGEX = /^(?:0|[1-9]\d*)\.\d{2}$/;
const ISO_4217_CURRENCY_REGEX = /^[A-Z]{3}$/;

/**
 * Value Object imutável que representa valores monetários com precisão decimal exata (Seção 6.1).
 * Nunca utiliza number/float para evitar erros de arredondamento IEEE-754.
 */
export class Money {
  /**
   * Construtor privado para forçar o uso das factories estáticas (from, zero)
   * e congelar a instância em memória garantindo imutabilidade.
   */
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {
    Object.freeze(this);
  }

  /**
   * Cria uma instância de Money a partir de um contrato de entrada (MoneyProps).
   * Rejeita NaN, Infinity, notação científica, string vazia, escala != 2 casas decimais e valores negativos.
   */
  static from(props: MoneyProps): Money {
    if (!props || typeof props.amount !== 'string' || typeof props.currency !== 'string') {
      throw new InvalidMoneyError('Money requires string amount and currency.');
    }

    if (!ISO_4217_CURRENCY_REGEX.test(props.currency)) {
      throw new InvalidMoneyError(
        `Invalid ISO-4217 currency code: "${props.currency}". Expected 3 uppercase letters (e.g., "BRL").`,
      );
    }

    if (!STRICT_NON_NEGATIVE_AMOUNT_REGEX.test(props.amount)) {
      throw new InvalidMoneyError(
        `Invalid monetary amount "${props.amount}". Must be a non-negative decimal string with fixed scale of 2 decimal places (e.g., "25.00").`,
      );
    }

    const decimalValue = new Decimal(props.amount);
    if (!decimalValue.isFinite() || decimalValue.isNegative()) {
      throw new InvalidMoneyError(`Amount "${props.amount}" must be finite and non-negative.`);
    }

    return new Money(decimalValue, props.currency);
  }

  /**
   * Fábrica interna que suporta valores assinados (ex.: resultado de subtract/negate ou diferença de reconciliação).
   */
  static fromSignedDecimalString(amount: string, currency: string): Money {
    if (!ISO_4217_CURRENCY_REGEX.test(currency)) {
      throw new InvalidMoneyError(`Invalid ISO-4217 currency code: "${currency}".`);
    }
    if (!/^-?(?:0|[1-9]\d*)\.\d{2}$/.test(amount)) {
      throw new InvalidMoneyError(
        `Invalid signed monetary amount "${amount}". Expected fixed scale of 2 decimal places.`,
      );
    }
    return new Money(new Decimal(amount), currency);
  }

  /**
   * Cria uma instância de Money com valor "0.00" para a moeda informada.
   */
  static zero(currency: string): Money {
    if (!ISO_4217_CURRENCY_REGEX.test(currency)) {
      throw new InvalidMoneyError(`Invalid ISO-4217 currency code: "${currency}".`);
    }
    return new Money(new Decimal('0.00'), currency);
  }

  /**
   * Retorna a representação textual do valor sempre formatada com exatamente 2 casas decimais.
   */
  get amount(): string {
    return this.value.toFixed(2);
  }

  /**
   * Soma este Money com outro de mesma moeda e retorna uma nova instância imutável.
   */
  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.plus(other.value), this.currency);
  }

  /**
   * Subtrai outro Money (de mesma moeda) deste valor e retorna uma nova instância imutável.
   */
  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.value.minus(other.value), this.currency);
  }

  /**
   * Inverte o sinal do valor monetário (mantendo "0.00" sem sinal negativo quando zero).
   */
  negate(): Money {
    const negated = this.value.isZero() ? new Decimal('0.00') : this.value.negated();
    return new Money(negated, this.currency);
  }

  /**
   * Verifica se o valor monetário é exatamente igual a zero ("0.00").
   */
  isZero(): boolean {
    return this.value.isZero();
  }

  /**
   * Verifica se o valor monetário é estritamente maior que zero (> 0).
   */
  isPositive(): boolean {
    return this.value.greaterThan(0);
  }

  /**
   * Verifica se o valor monetário é estritamente menor que zero (< 0).
   */
  isNegative(): boolean {
    return this.value.lessThan(0);
  }

  /**
   * Verifica se este valor é estritamente menor que outro Money da mesma moeda.
   */
  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lessThan(other.value);
  }

  /**
   * Verifica se este valor é estritamente maior que outro Money da mesma moeda.
   */
  isGreaterThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.greaterThan(other.value);
  }

  /**
   * Compara igualdade de valor e moeda com outra instância de Money.
   */
  equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.equals(other.value);
  }

  /**
   * Serializa a instância para o formato de contrato JSON { amount, currency }.
   */
  toJSON(): MoneyProps {
    return {
      amount: this.value.toFixed(2),
      currency: this.currency,
    };
  }

  /**
   * Retorna uma representação legível no formato "25.00 BRL".
   */
  toString(): string {
    return `${this.value.toFixed(2)} ${this.currency}`;
  }

  /**
   * Garante que operações aritméticas ou comparações só ocorram entre moedas idênticas.
   * Lança CurrencyMismatchError caso contrário.
   */
  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }
}
