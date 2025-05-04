import * as path from 'node:path';
export const SERVER_PORT = process.env.SERVER_PORT || 3001;
export const DEFAULT_INPUT_PATH =
  process.env.DEFAULT_INPUT_PATH ||
  path.join('storage', 'input', 'orders.json');

export const DEFAULT_ORDER_BOOK_PATH =
  process.env.DEFAULT_ORDER_BOOK_PATH ||
  path.join('storage', 'output', 'orderbook.json');

export const DEFAULT_TRADE_BOOK_PATH =
  process.env.DEFAULT_TRADE_BOOK_PATH ||
  path.join('storage', 'output', 'trades.json');
