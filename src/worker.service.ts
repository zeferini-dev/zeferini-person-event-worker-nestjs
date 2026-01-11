import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Channel, Connection, connect } from 'amqplib';
import { Pool } from 'pg';

interface EventRow {
  id: string;
  aggregateId: string;
  aggregateType: string;
  eventType: string;
  eventData: any;
  metadata: any;
  version: number;
  timestamp: Date;
  createdAt: Date;
}

@Injectable()
export class WorkerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(WorkerService.name);
  private pool: Pool;
  private connection: Connection | null = null;
  private channel: Channel | null = null;
  private interval: NodeJS.Timeout | null = null;

  private readonly queue: string;
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;

  constructor(private readonly config: ConfigService) {
    this.queue = this.config.get<string>('RABBITMQ_QUEUE', 'person_events');
    this.pollIntervalMs = Number(this.config.get<string>('POLL_INTERVAL_MS', '2000'));
    this.batchSize = Number(this.config.get<string>('BATCH_SIZE', '100'));

    const eventsDbUrl = this.config.get<string>(
      'EVENTS_DATABASE_URL',
      'postgresql://events:events123@localhost:5433/eventstore',
    );
    this.pool = new Pool({ connectionString: eventsDbUrl });
  }

  async onModuleInit() {
    await this.connectRabbit();
    await this.ensureQueue();

    await this.pollOnce();
    this.interval = setInterval(() => this.pollOnce().catch((err) => this.logger.error(err)), this.pollIntervalMs);
    this.logger.log(`Worker running. Polling every ${this.pollIntervalMs} ms (batch ${this.batchSize}).`);
  }

  async onModuleDestroy() {
    if (this.interval) {
      clearInterval(this.interval);
    }
    if (this.channel) {
      await this.channel.close().catch(() => undefined);
      this.channel = null;
    }
    if (this.connection) {
      await (this.connection as any)?.close?.().catch(() => undefined);
      this.connection = null;
    }
    await this.pool.end().catch(() => undefined);
  }

  private async connectRabbit() {
    const url = this.config.get<string>('RABBITMQ_URL', 'amqp://admin:admin123@localhost:5672');
    const maxRetries = 10;
    let retries = 0;
    let lastError: Error | null = null;

    while (retries < maxRetries) {
      try {
        this.connection = (await connect(url)) as unknown as Connection;
        this.channel = await (this.connection as any).createChannel();
        this.logger.log(`Connected to RabbitMQ (${url.replace(/:[^:@]+@/, ':****@')})`);
        return;
      } catch (error) {
        lastError = error as Error;
        retries++;
        const delayMs = Math.min(1000 * Math.pow(2, retries - 1), 30000);
        this.logger.warn(
          `RabbitMQ connection failed (attempt ${retries}/${maxRetries}). Retrying in ${delayMs}ms...`,
          lastError.message,
        );
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }

    throw new Error(`Failed to connect to RabbitMQ after ${maxRetries} attempts: ${lastError?.message}`);
  }

  private async ensureQueue() {
    if (!this.channel) throw new Error('Channel not initialized');
    await this.channel.assertQueue(this.queue, { durable: true });
  }

  private parseJson(value: any) {
    if (value === null || value === undefined) return value;
    if (typeof value === 'string') {
      try {
        return JSON.parse(value);
      } catch {
        return value;
      }
    }
    return value;
  }

  private async fetchUndispatched(limit: number): Promise<EventRow[]> {
    const client = await this.pool.connect();
    try {
      const query = `
        SELECT 
          id,
          "aggregateId",
          "aggregateType",
          "eventType",
          "eventData",
          metadata,
          version,
          timestamp,
          "createdAt"
        FROM eventstore.events
        WHERE COALESCE(metadata->>'dispatched', 'false') <> 'true'
        ORDER BY "createdAt" ASC
        LIMIT $1
      `;

      const result = await client.query(query, [limit]);

      return result.rows.map((row) => ({
        ...row,
        eventData: this.parseJson(row.eventData),
        metadata: this.parseJson(row.metadata),
      }));
    } finally {
      client.release();
    }
  }

  private async markDispatched(ids: string[]): Promise<void> {
    if (ids.length === 0) return;
    const client = await this.pool.connect();
    try {
      const query = `
        UPDATE eventstore.events
        SET metadata = jsonb_set(
          jsonb_set(COALESCE(metadata, '{}')::jsonb, '{dispatched}', 'true'::jsonb, true),
          '{dispatchedAt}', to_jsonb(now())
        )
        WHERE id = ANY($1::uuid[])
      `;
      await client.query(query, [ids]);
    } finally {
      client.release();
    }
  }

  private async pollOnce(): Promise<void> {
    if (!this.channel) throw new Error('Channel not initialized');

    const rows = await this.fetchUndispatched(this.batchSize);
    if (!rows.length) return;

    for (const evt of rows) {
      const payload = {
        id: evt.id,
        aggregateId: evt.aggregateId,
        aggregateType: evt.aggregateType,
        eventType: evt.eventType,
        eventData: evt.eventData,
        metadata: evt.metadata,
        version: evt.version,
        timestamp: evt.timestamp,
        createdAt: evt.createdAt,
      };

      this.channel.sendToQueue(this.queue, Buffer.from(JSON.stringify(payload)), {
        persistent: true,
        contentType: 'application/json',
      });
    }

    await this.markDispatched(rows.map((r) => r.id));
    this.logger.log(`Dispatched ${rows.length} events to queue '${this.queue}'.`);
  }
}
