import { EventEmitter } from 'events';
import { Horizon, Server } from '@stellar/stellar-sdk';

export interface HorizonListenerConfig {
  horizonUrl: string;
  contracts: string[];
  /**
   * Optional topic filter. Only events whose first topic symbol matches one of
   * these entries are forwarded. An entry matches exactly when it equals the
   * symbol, or as a prefix when it ends with `*` (e.g. `transfer*`).
   */
  topicFilter?: string[];
  pollIntervalMs?: number;
  cursor?: string;
}

export interface ContractEvent {
  contractId: string;
  topics: string[];
  value: unknown;
  ledger: number;
  txHash: string;
  pagingToken: string;
}

/**
 * Returns true when the event's first topic matches the configured filter.
 * A filter entry matches exactly when it equals the symbol, or as a prefix
 * when it ends with `*`. An empty/undefined filter forwards everything.
 */
export function matchesTopicFilter(
  topics: string[],
  filter?: string[],
): boolean {
  if (!filter || filter.length === 0) {
    return true;
  }
  const symbol = topics[0];
  if (symbol === undefined) {
    return false;
  }
  return filter.some((entry) => {
    if (entry.endsWith('*')) {
      return symbol.startsWith(entry.slice(0, -1));
    }
    return symbol === entry;
  });
}

export class HorizonListener extends EventEmitter {
  private readonly config: Required<Pick<HorizonListenerConfig, 'horizonUrl' | 'contracts' | 'pollIntervalMs'>> &
    HorizonListenerConfig;
  private server: Server;
  private cursor?: string;
  private timer?: NodeJS.Timeout;
  private running = false;

  constructor(config: HorizonListenerConfig) {
    super();
    this.config = {
      pollIntervalMs: 5000,
      ...config,
    };
    this.server = new Server(this.config.horizonUrl);
    this.cursor = config.cursor;
  }

  start(): void {
    if (this.running) {
      return;
    }
    this.running = true;
    void this.poll();
  }

  stop(): void {
    this.running = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
  }

  private async poll(): Promise<void> {
    if (!this.running) {
      return;
    }
    try {
      await this.fetchEvents();
    } catch (err) {
      this.emit('error', err);
    } finally {
      if (this.running) {
        this.timer = setTimeout(() => void this.poll(), this.config.pollIntervalMs);
      }
    }
  }

  private async fetchEvents(): Promise<void> {
    for (const contractId of this.config.contracts) {
      const builder = this.server
        .events()
        .forContract(contractId)
        .order('asc')
        .limit(200);
      if (this.cursor) {
        builder.cursor(this.cursor);
      }
      const response = await builder.call();
      for (const raw of response.records) {
        const event = this.toContractEvent(raw);
        this.cursor = event.pagingToken;
        if (!matchesTopicFilter(event.topics, this.config.topicFilter)) {
          continue;
        }
        this.emit('event', event);
      }
    }
  }

  private toContractEvent(raw: Horizon.ServerApi.EventRecord): ContractEvent {
    return {
      contractId: raw.contractId ?? '',
      topics: (raw.topic ?? []).map((t) => String(t)),
      value: raw.value,
      ledger: raw.ledger,
      txHash: raw.txHash,
      pagingToken: raw.pagingToken,
    };
  }
}
