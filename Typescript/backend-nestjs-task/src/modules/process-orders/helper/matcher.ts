import { Heap } from 'heap-js';
import Big from 'big.js';
import { BookOrder, RawOrder, Trade } from '../types/trade';

class OrderBook {
  private bids: Heap<BookOrder>;
  private asks: Heap<BookOrder>;
  private idIndex = new Map<string, BookOrder>();
  private seq = 0;

  public trades: Trade[] = [];

  constructor(private readonly pair: string) {
    // Bid max-heap — highest price first, FIFO on equal price
    this.bids = new Heap<BookOrder>((a, b) =>
      a.price.eq(b.price) ? a.ts - b.ts : b.price.cmp(a.price),
    );
    // Ask min-heap — lowest price first, FIFO on equal price
    this.asks = new Heap<BookOrder>((a, b) =>
      a.price.eq(b.price) ? a.ts - b.ts : a.price.cmp(b.price),
    );
  }

  process(raw: RawOrder) {
    if (raw.type_op === 'DELETE') {
      const found = this.idIndex.get(raw.order_id);
      if (found) {
        this.remove(found);
      }
      return;
    }

    const order: BookOrder = {
      id: raw.order_id,
      account: raw.account_id,
      side: raw.side,
      pair: raw.pair,
      price: new Big(raw.limit_price),
      remaining: new Big(raw.amount),
      ts: this.seq++,
    };
    this.match(order);
    if (order.remaining.gt(0)) {
      this.add(order);
    }
  }

  private add(order: BookOrder) {
    this.idIndex.set(order.id, order);
    (order.side === 'BUY' ? this.bids : this.asks).push(order);
  }

  private remove(order: BookOrder) {
    if (order.side === 'BUY') this.bids.remove(order);
    else this.asks.remove(order);
    this.idIndex.delete(order.id);
  }

  private match(incoming: BookOrder) {
    const isBuy = incoming.side === 'BUY';
    const bookSide = isBuy ? this.asks : this.bids;

    while (incoming.remaining.gt(0) && bookSide.size()) {
      const best = bookSide.peek()!;
      const priceOK = isBuy
        ? incoming.price.gte(best.price)
        : incoming.price.lte(best.price);
      if (!priceOK) break;

      const tradeQty = incoming.remaining.lt(best.remaining)
        ? incoming.remaining
        : best.remaining; // Big.min(incoming.remaining, best.remaining);
      const tradePrice = best.price;
      const trade: Trade = {
        pair: this.pair,
        buyOrderId: isBuy ? incoming.id : best.id,
        sellOrderId: isBuy ? best.id : incoming.id,
        price: tradePrice.toString(),
        amount: tradeQty.toString(),
        ts: Date.now(),
      };
      this.trades.push(trade);

      incoming.remaining = incoming.remaining.minus(tradeQty);
      best.remaining = best.remaining.minus(tradeQty);

      if (best.remaining.eq(0)) {
        this.remove(best);
      }
    }
  }

  normalize() {
    return {
      pair: this.pair,
      bids: [...this.bids.toArray()].map((o) => ({
        id: o.id,
        account: o.account,
        price: o.price.toString(),
        remaining: o.remaining.toString(),
      })),
      asks: [...this.asks.toArray()].map((o) => ({
        id: o.id,
        price: o.price.toString(),
        remaining: o.remaining.toString(),
        account: o.account,
      })),
    };
  }
}

export class MatcherEngine {
  private books = new Map<string, OrderBook>();
  private trades: Trade[] = [];

  private bookFor(pair: string) {
    let book = this.books.get(pair);
    if (!book) {
      book = new OrderBook(pair);
      this.books.set(pair, book);
    }
    return book;
  }

  ingest(raw: RawOrder) {
    const book = this.bookFor(raw.pair);
    book.process(raw);
  }

  finish() {
    this.trades = Array.from(this.books.values()).flatMap((b) => b.trades);
    return {
      orderbooks: Array.from(this.books.values()).map((b) => b.normalize()),
      trades: this.trades,
    };
  }
}
