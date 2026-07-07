import * as net from 'net';
import {
  MessageEnvelope,
  MessageType,
  ProduceRequest,
  ProduceResponse,
  ConsumeRequest,
  ConsumeResponse,
  StoredMessage,
  CommitOffsetRequest,
  CommitOffsetResponse,
  FetchOffsetRequest,
  FetchOffsetResponse,
  NackRequest,
  NackResponse,
  ProduceBatchRequest,
  ProduceBatchResponse
} from './messages';

export class DRMQConnectionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DRMQConnectionError';
  }
}

export class DRMQClient {
  protected bootstrapServers: { host: string; port: number }[];
  protected currentServerIndex: number;
  protected host: string;
  protected port: number;
  protected socket: net.Socket | null = null;
  private responseQueue: Array<(data: Buffer) => void> = [];
  private receiveBuffer: Buffer = Buffer.alloc(0);
  protected maxRetries = 5;

  constructor(bootstrapServers: string) {
    this.bootstrapServers = bootstrapServers.split(',').map(s => {
      const [host, port] = s.trim().split(':');
      return { host, port: parseInt(port, 10) };
    });
    this.currentServerIndex = Math.floor(Math.random() * this.bootstrapServers.length);
    this.host = this.bootstrapServers[this.currentServerIndex].host;
    this.port = this.bootstrapServers[this.currentServerIndex].port;
  }

  public async connect(): Promise<void> {
    await this.ensureConnected();
  }

  protected async ensureConnected(): Promise<void> {
    if (this.socket) return;

    let lastError: Error | null = null;
    const totalAttempts = this.maxRetries * Math.max(1, this.bootstrapServers.length);

    for (let attempt = 0; attempt < totalAttempts; attempt++) {
      try {
        await new Promise<void>((resolve, reject) => {
          this.socket = new net.Socket();
          this.socket.setTimeout(5000);
          this.socket.connect(this.port, this.host, () => {
            resolve();
          });

          this.socket.on('data', (data) => this.handleData(data as Buffer));
          
          this.socket.on('error', (err) => {
            if (!this.socket?.connecting) this.close();
            reject(err);
          });
          
          this.socket.on('close', () => {
            this.socket = null;
          });

          this.socket.on('timeout', () => {
            this.close();
            reject(new Error("Connection timeout"));
          });
        });
        return; // Connected successfully
      } catch (err) {
        lastError = err as Error;
        this.closeConnection();
        this.rotateServer();
        await new Promise(res => setTimeout(res, 500)); // sleep
      }
    }
    throw new DRMQConnectionError(`Failed to connect after ${totalAttempts} attempts. Last error: ${lastError?.message}`);
  }

  protected rotateServer(): void {
    if (this.bootstrapServers.length <= 1) return;
    this.currentServerIndex = (this.currentServerIndex + 1) % this.bootstrapServers.length;
    this.host = this.bootstrapServers[this.currentServerIndex].host;
    this.port = this.bootstrapServers[this.currentServerIndex].port;
  }

  protected async reconnect(): Promise<void> {
    this.closeConnection();
    this.rotateServer();
    await this.ensureConnected();
  }

  protected async tryRedirectToLeader(errorMessage: string | undefined | null): Promise<boolean> {
    if (!errorMessage || !errorMessage.startsWith("NOT_LEADER:")) {
      return false;
    }
    
    const leaderInfo = errorMessage.substring("NOT_LEADER:".length);
    if (leaderInfo !== "UNKNOWN") {
      const parts = leaderInfo.split(":");
      if (parts.length === 2) {
        this.host = parts[0];
        this.port = parseInt(parts[1], 10);
        this.closeConnection();
        await this.ensureConnected();
        return true;
      }
    }
    
    await this.reconnect();
    return true;
  }

  protected closeConnection(): void {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
    this.responseQueue.forEach(resolve => resolve(Buffer.alloc(0)));
    this.responseQueue = [];
    this.receiveBuffer = Buffer.alloc(0);
  }

  public close(): void {
    this.closeConnection();
  }

  private handleData(data: Buffer): void {
    this.receiveBuffer = Buffer.concat([this.receiveBuffer, data]);

    while (this.receiveBuffer.length >= 4) {
      const length = this.receiveBuffer.readUInt32BE(0);
      
      if (this.receiveBuffer.length >= 4 + length) {
        const frameData = this.receiveBuffer.subarray(4, 4 + length);
        this.receiveBuffer = this.receiveBuffer.subarray(4 + length);
        
        const resolve = this.responseQueue.shift();
        if (resolve) resolve(frameData);
      } else {
        break;
      }
    }
  }

  protected async sendEnvelope(msgType: MessageType, payload: Uint8Array): Promise<Uint8Array> {
    await this.ensureConnected();

    const envelope = MessageEnvelope.create({
      type: msgType,
      payload: Buffer.from(payload)
    });
    const envelopeBytes = MessageEnvelope.encode(envelope).finish();

    const lengthPrefix = Buffer.alloc(4);
    lengthPrefix.writeUInt32BE(envelopeBytes.length, 0);

    return new Promise((resolve, reject) => {
      if (!this.socket) return reject(new Error('Socket disconnected'));
      
      this.responseQueue.push((frameData: Buffer) => {
        if (frameData.length === 0) {
          reject(new Error("Connection closed while waiting for response"));
          return;
        }
        try {
          const respEnvelope = MessageEnvelope.decode(frameData);
          resolve(respEnvelope.payload);
        } catch (e) {
          reject(e);
        }
      });

      this.socket.write(Buffer.concat([lengthPrefix, envelopeBytes]), (err) => {
        if (err) reject(err);
      });
    });
  }
}

export interface SendResult {
  success: boolean;
  offset: number;
  errorMessage?: string;
}

interface PendingMessage {
  payload: Uint8Array;
  key?: string;
  timestamp: number;
  resolve: (res: SendResult) => void;
  reject: (err: Error) => void;
}

export class DRMQProducer extends DRMQClient {
  private accumulator: Map<string, PendingMessage[]> = new Map();
  private sendTimer: NodeJS.Timeout | null = null;
  private isClosed = false;

  public async send(topic: string, payload: Uint8Array, key?: string): Promise<SendResult> {
    if (this.isClosed) throw new Error("Producer is closed");
    
    return new Promise((resolve, reject) => {
      if (!this.accumulator.has(topic)) {
        this.accumulator.set(topic, []);
      }
      this.accumulator.get(topic)!.push({
        payload: new Uint8Array(payload),
        key,
        timestamp: Date.now(),
        resolve,
        reject
      });

      if (!this.sendTimer) {
        this.sendTimer = setTimeout(() => this.flush(), 5);
      }
    });
  }

  private async flush() {
    this.sendTimer = null;
    const batches = this.accumulator;
    this.accumulator = new Map();

    for (const [topic, batch] of batches.entries()) {
      this.sendBatchWithRetry(topic, batch).catch(e => {
        batch.forEach(pm => pm.reject(e));
      });
    }
  }

  private async sendBatchWithRetry(topic: string, batch: PendingMessage[]) {
    const req = ProduceBatchRequest.create({
      topic,
      entries: batch.map(pm => ({
        payload: Buffer.from(pm.payload),
        key: pm.key,
        clientTimestamp: pm.timestamp
      }))
    });

    const envelopeBytes = ProduceBatchRequest.encode(req).finish();
    const deliveryTimeoutMs = 120000;
    const startMs = Date.now();
    let currentBackoffMs = 100;

    while (true) {
      if (Date.now() - startMs > deliveryTimeoutMs) {
        break;
      }

      try {
        await this.ensureConnected();
        const respBytes = await this.sendEnvelope(MessageType.PRODUCE_BATCH_REQUEST, envelopeBytes);
        const resp = ProduceBatchResponse.decode(respBytes);

        if (resp.success) {
          let baseOffset = Number(resp.baseOffset);
          batch.forEach((pm, i) => pm.resolve({ success: true, offset: baseOffset + i }));
          return;
        } else {
          const errorMsg = resp.errorMessage;
          if (errorMsg && errorMsg.startsWith("NOT_LEADER:")) {
            const leaderAddr = errorMsg.substring("NOT_LEADER:".length);
            if (leaderAddr !== "UNKNOWN") {
              const parts = leaderAddr.split(":");
              if (parts.length === 2) {
                this.host = parts[0];
                this.port = parseInt(parts[1], 10);
                super.closeConnection();
                await this.ensureConnected();
                continue;
              }
            }
            super.closeConnection();
            this.rotateServer();
          } else if (errorMsg && (errorMsg.includes("timed out") || errorMsg.includes("Lost leadership") || errorMsg.includes("Raft batch proposal"))) {
            super.closeConnection();
            this.rotateServer();
          } else {
            batch.forEach(pm => pm.resolve({ success: false, offset: -1, errorMessage: errorMsg }));
            return;
          }
        }
      } catch (e) {
        super.closeConnection();
        this.rotateServer();
      }

      await new Promise(res => setTimeout(res, currentBackoffMs));
      currentBackoffMs = Math.min(2000, currentBackoffMs * 2);
    }

    throw new Error("Failed to send batch: timeout exhausted");
  }

  public close() {
    this.isClosed = true;
    if (this.sendTimer) clearTimeout(this.sendTimer);
    super.close();
  }
}

export class DRMQConsumer extends DRMQClient {
  private groupId?: string;
  private consumerId: string;
  private subscriptions: string[] = [];
  private localOffsets: Record<string, number> = {};
  public autoCommit: boolean = false;
  private groupMode: boolean;

  constructor(bootstrapServers: string, groupId?: string, consumerId: string = 'ts-consumer-1') {
    super(bootstrapServers);
    this.groupId = groupId;
    this.consumerId = consumerId;
    this.groupMode = groupId !== undefined;
  }

  public async subscribe(topic: string, fromOffset?: number): Promise<void> {
    if (!this.subscriptions.includes(topic)) {
      this.subscriptions.push(topic);
    }
    
    if (this.groupMode) {
      this.localOffsets[topic] = -1; // Broker manages it
    } else {
      if (fromOffset !== undefined) {
        this.localOffsets[topic] = fromOffset;
      } else {
        this.localOffsets[topic] = await this.fetchOffset(topic);
      }
    }
  }

  public async poll(maxMessages: number = 100, timeoutMs: number = 1000): Promise<StoredMessage[]> {
    for (let attempt = 0; attempt < this.maxRetries; attempt++) {
      try {
        await this.ensureConnected();
        const allMessages: StoredMessage[] = [];

        for (const topic of this.subscriptions) {
          const req = ConsumeRequest.create({
            topic,
            maxMessages,
            timeoutMs
          });

          if (this.groupMode) {
            req.consumerGroup = this.groupId;
            req.consumerId = this.consumerId;
          } else {
            req.fromOffset = this.localOffsets[topic] || 0;
          }

          const respBytes = await this.sendEnvelope(
            MessageType.CONSUME_REQUEST,
            ConsumeRequest.encode(req).finish()
          );

          const resp = ConsumeResponse.decode(respBytes);
          
          if (resp.success) {
            if (resp.messages.length > 0) {
              allMessages.push(...resp.messages);
              const nextOffset = resp.messages[resp.messages.length - 1].offset + 1;
              this.localOffsets[topic] = nextOffset;
              
              if (this.autoCommit) {
                await this.commit(topic, nextOffset);
              }
            }
          } else if (await this.tryRedirectToLeader(resp.errorMessage)) {
             throw new Error("Redirected to leader"); // Break loop to retry from outer layer
          }
        }

        return allMessages;
      } catch (err) {
        await this.reconnect();
      }
    }
    throw new DRMQConnectionError(`Failed to poll messages after ${this.maxRetries} attempts`);
  }

  private async fetchOffset(topic: string): Promise<number> {
    for (let attempt = 0; attempt < this.maxRetries; attempt++) {
      try {
        await this.ensureConnected();
        const req = FetchOffsetRequest.create({
          consumerGroup: this.groupId || "single-mode-external",
          topic
        });
        
        const respBytes = await this.sendEnvelope(
          MessageType.FETCH_OFFSET_REQUEST,
          FetchOffsetRequest.encode(req).finish()
        );
        
        const resp = FetchOffsetResponse.decode(respBytes);
        
        if (!resp.success && await this.tryRedirectToLeader(resp.errorMessage)) {
          continue;
        }
        
        return Math.max(0, resp.offset);
      } catch (err) {
        await this.reconnect();
      }
    }
    return 0;
  }

  public async commit(topic: string, nextOffset: number): Promise<void> {
    for (let attempt = 0; attempt < this.maxRetries; attempt++) {
      try {
        await this.ensureConnected();
        const req = CommitOffsetRequest.create({
          consumerGroup: this.groupId || "single-mode-external",
          topic,
          offset: nextOffset,
          consumerId: this.groupMode ? this.consumerId : undefined
        });

        const respBytes = await this.sendEnvelope(
          MessageType.COMMIT_OFFSET_REQUEST,
          CommitOffsetRequest.encode(req).finish()
        );

        const resp = CommitOffsetResponse.decode(respBytes);
        
        if (resp.success) {
          this.localOffsets[topic] = nextOffset;
          return;
        } else if (await this.tryRedirectToLeader(resp.errorMessage)) {
          continue;
        }
      } catch (err) {
        await this.reconnect();
      }
    }
  }

  public async nack(topic: string, offset: number): Promise<boolean> {
    if (!this.groupMode) {
      throw new Error("NACK is only supported in consumer group mode");
    }

    for (let attempt = 0; attempt < this.maxRetries; attempt++) {
      try {
        await this.ensureConnected();
        const req = NackRequest.create({
          consumerGroup: this.groupId || "single-mode-external",
          topic,
          offset,
          consumerId: this.groupMode ? this.consumerId : undefined
        });

        const respBytes = await this.sendEnvelope(
          MessageType.NACK_REQUEST,
          NackRequest.encode(req).finish()
        );

        const resp = NackResponse.decode(respBytes);
        
        if (resp.success) {
          return resp.routedToDlq;
        } else if (await this.tryRedirectToLeader(resp.errorMessage)) {
          continue;
        } else {
          throw new DRMQConnectionError(`NACK failed: ${resp.errorMessage}`);
        }
      } catch (err) {
        if (err instanceof DRMQConnectionError && err.message.includes("NACK failed")) {
          throw err;
        }
        await this.reconnect();
      }
    }
    throw new DRMQConnectionError(`Failed to nack message after ${this.maxRetries} attempts`);
  }
}
