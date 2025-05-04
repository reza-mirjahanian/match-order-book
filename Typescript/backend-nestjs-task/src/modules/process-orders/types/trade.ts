import Big from 'big.js';

export type Side = 'BUY' | 'SELL';

export interface RawOrder {
  type_op: 'CREATE' | 'DELETE';
  account_id: string;
  amount: string;
  order_id: string;
  pair: string;
  limit_price: string;
  side: Side;
}

export interface BookOrder {
  id: string;
  account: string;
  side: Side;
  pair: string;
  price: Big;
  remaining: Big;
  ts: number; // time-sequence for FIFO
}

export interface Trade {
  pair: string;
  buyOrderId: string;
  sellOrderId: string;
  price: string;
  amount: string;
  ts: number;
}

export interface Order {
  pair: string;
  bids: {
    id: string;
    price: string;
    remaining: string;
    account: string;
  }[];
  asks: {
    id: string;
    price: string;
    remaining: string;
    account: string;
  }[];
}
