import { Injectable, Logger } from '@nestjs/common';
import * as fs from 'fs';
import { pipeline } from 'stream/promises';
import { parser } from 'stream-json';
import { streamArray } from 'stream-json/streamers/StreamArray.js';
import { MatcherEngine } from './helper/matcher';
import { Trade, RawOrder, Order } from './types/trade';
import { Readable, Writable } from 'stream';
import {
  DEFAULT_INPUT_PATH,
  DEFAULT_ORDER_BOOK_PATH,
  DEFAULT_TRADE_BOOK_PATH,
} from '../../constants/server';

@Injectable()
export class ProcessOrderService {
  private readonly logger = new Logger(ProcessOrderService.name);

  private defaultInputPath = DEFAULT_INPUT_PATH;
  private defaultOrderBookPath = DEFAULT_ORDER_BOOK_PATH;
  private defaultTradeBookPath = DEFAULT_TRADE_BOOK_PATH;

  storeResult(
    orderbooks: Order[],
    trades: Trade[],
    orderBookPath = this.defaultOrderBookPath,
    tradeBookPath = this.defaultTradeBookPath,
  ): void {
    fs.writeFileSync(orderBookPath, JSON.stringify(orderbooks, null, 2));
    fs.writeFileSync(tradeBookPath, JSON.stringify(trades, null, 2));
    console.log(`✅ Done → ${orderBookPath} | ${tradeBookPath}`);
  }

  async runFromFile(
    inputPath = this.defaultInputPath,
  ): Promise<{ orderbooks: Order[]; trades: Trade[] }> {
    try {
      const inputStream = fs.createReadStream(inputPath);
      const result = await this.process(inputStream);
      this.storeResult(result.orderbooks, result.trades);
      return result;
    } catch (err) {
      console.error('❌ Error processing orders!', err);
      this.logger.error('Error in process-order.service.run() !');
      throw err;
    }
  }

  async runFromJson(
    orders: RawOrder[],
  ): Promise<{ orderbooks: Order[]; trades: Trade[] }> {
    try {
      const inputStream = Readable.from([JSON.stringify(orders)]);
      return await this.process(inputStream);
    } catch (err) {
      console.error('❌ Error processing orders!', err);
      this.logger.error('Error in process-order.service.runFromJson() !');
      throw err;
    }
  }

  async process(
    inputStream: Readable,
  ): Promise<{ orderbooks: Order[]; trades: Trade[] }> {
    const engine = new MatcherEngine();

    const ingestStream = new Writable({
      objectMode: true,
      write(chunk, _enc, callback) {
        const { value } = chunk as { key: number; value: RawOrder };
        engine.ingest(value);
        callback();
      },
    });

    await pipeline(inputStream, parser(), streamArray(), ingestStream);

    return engine.finish();
  }
}
