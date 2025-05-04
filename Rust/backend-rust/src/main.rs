use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap};
use std::fs;
use std::str::FromStr;

#[derive(Deserialize)]
pub enum Operation {
    CREATE,
    DELETE,
}

#[derive(Deserialize, Serialize, Clone, Eq, PartialEq)]
pub enum Side {
    BUY,
    SELL,
}

#[derive(Deserialize)]
pub struct RawOrder {
    type_op: Operation,
    account_id: String,
    amount: String,
    order_id: String,
    pair: String,
    limit_price: String,
    side: Side,
}

#[derive(Clone, Eq, PartialEq)]
pub struct BookOrder {
    id: String,
    account: String,
    side: Side,
    pair: String,
    price: Decimal,
    remaining: Decimal,
    ts: u64,
}

#[derive(Serialize, Clone)]
pub struct Trade {
    pair: String,
    #[serde(rename = "buyOrderId")]
    buy_order_id: String,
    #[serde(rename = "sellOrderId")]
    sell_order_id: String,
    price: String,
    amount: String,
    ts: u64,
}

#[derive(Serialize)]
pub struct Order {
    pair: String,
    bids: Vec<Bid>,
    asks: Vec<Ask>,
}

#[derive(Serialize)]
pub struct Bid {
    id: String,
    price: String,
    remaining: String,
    account: String,
}

#[derive(Serialize)]
pub struct Ask {
    id: String,
    price: String,
    remaining: String,
    account: String,
}

#[derive(Eq, PartialEq)]
struct BidBookOrder(BookOrder);

impl Ord for BidBookOrder {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .price
            .cmp(&other.0.price)
            .then_with(|| other.0.ts.cmp(&self.0.ts))
    }
}

impl PartialOrd for BidBookOrder {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Eq, PartialEq)]
struct AskBookOrder(BookOrder);

impl Ord for AskBookOrder {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .price
            .cmp(&other.0.price)
            .then_with(|| self.0.ts.cmp(&other.0.ts))
    }
}

impl PartialOrd for AskBookOrder {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct OrderBook {
    pair: String,
    bids: BinaryHeap<BidBookOrder>,
    asks: BinaryHeap<Reverse<AskBookOrder>>,
    id_index: HashMap<String, BookOrder>,
    seq: u64,
    trades: Vec<Trade>,
}

impl OrderBook {
    fn new(pair: String) -> Self {
        OrderBook {
            pair,
            bids: BinaryHeap::new(),
            asks: BinaryHeap::new(),
            id_index: HashMap::new(),
            seq: 0,
            trades: Vec::new(),
        }
    }

    fn process(&mut self, raw: RawOrder) {
        if matches!(raw.type_op, Operation::DELETE) {
            self.id_index.remove(&raw.order_id);
            return;
        }

        let price = Decimal::from_str(&raw.limit_price).expect("Invalid limit_price");
        let amount = Decimal::from_str(&raw.amount).expect("Invalid amount");
        let mut order = BookOrder {
            id: raw.order_id,
            account: raw.account_id,
            side: raw.side,
            pair: raw.pair,
            price,
            remaining: amount,
            ts: self.seq,
        };
        self.seq += 1;
        self.match_order(&mut order);
        if order.remaining > Decimal::ZERO {
            self.add(order);
        }
    }

    fn add(&mut self, order: BookOrder) {
        self.id_index.insert(order.id.clone(), order.clone());
        match order.side {
            Side::BUY => self.bids.push(BidBookOrder(order)),
            Side::SELL => self.asks.push(Reverse(AskBookOrder(order))),
        }
    }

    fn match_order(&mut self, incoming: &mut BookOrder) {
        let is_buy = matches!(incoming.side, Side::BUY);
        if is_buy {
            while incoming.remaining > Decimal::ZERO {
                if let Some(mut best_order) = self.pop_active_top_asks() {
                    if incoming.price < best_order.price {
                        self.asks.push(Reverse(AskBookOrder(best_order)));
                        break;
                    }
                    let trade_qty = incoming.remaining.min(best_order.remaining);
                    let trade_price = best_order.price;
                    let trade = Trade {
                        pair: self.pair.clone(),
                        buy_order_id: incoming.id.clone(),
                        sell_order_id: best_order.id.clone(),
                        price: trade_price.to_string(),
                        amount: trade_qty.to_string(),
                        ts: self.seq,
                    };
                    self.trades.push(trade);
                    incoming.remaining -= trade_qty;
                    best_order.remaining -= trade_qty;
                    if best_order.remaining > Decimal::ZERO {
                        self.id_index
                            .insert(best_order.id.clone(), best_order.clone());
                        self.asks.push(Reverse(AskBookOrder(best_order)));
                    }
                } else {
                    break;
                }
            }
        } else {
            while incoming.remaining > Decimal::ZERO {
                if let Some(mut best_order) = self.pop_active_top_bids() {
                    if incoming.price > best_order.price {
                        self.bids.push(BidBookOrder(best_order));
                        break;
                    }
                    let trade_qty = incoming.remaining.min(best_order.remaining);
                    let trade_price = best_order.price;
                    let trade = Trade {
                        pair: self.pair.clone(),
                        buy_order_id: best_order.id.clone(),
                        sell_order_id: incoming.id.clone(),
                        price: trade_price.to_string(),
                        amount: trade_qty.to_string(),
                        ts: self.seq,
                    };
                    self.trades.push(trade);
                    incoming.remaining -= trade_qty;
                    best_order.remaining -= trade_qty;
                    if best_order.remaining > Decimal::ZERO {
                        self.id_index
                            .insert(best_order.id.clone(), best_order.clone());
                        self.bids.push(BidBookOrder(best_order));
                    }
                } else {
                    break;
                }
            }
        }
    }

    fn pop_active_top_asks(&mut self) -> Option<BookOrder> {
        while let Some(Reverse(AskBookOrder(order))) = self.asks.pop() {
            if let Some(active_order) = self.id_index.get(&order.id) {
                if active_order.remaining > Decimal::ZERO {
                    return Some(active_order.clone());
                }
            }
        }
        None
    }

    fn pop_active_top_bids(&mut self) -> Option<BookOrder> {
        while let Some(BidBookOrder(order)) = self.bids.pop() {
            if let Some(active_order) = self.id_index.get(&order.id) {
                if active_order.remaining > Decimal::ZERO {
                    return Some(active_order.clone());
                }
            }
        }
        None
    }

    fn normalize(&self) -> Order {
        let mut bids: Vec<_> = self
            .bids
            .iter()
            .filter_map(|BidBookOrder(order)| {
                self.id_index
                    .get(&order.id)
                    .filter(|o| o.remaining > Decimal::ZERO).cloned()
            })
            .collect();
        bids.sort_by(|a, b| b.price.cmp(&a.price).then_with(|| a.ts.cmp(&b.ts)));
        let bids = bids
            .into_iter()
            .map(|order| Bid {
                id: order.id.clone(),
                price: order.price.to_string(),
                remaining: order.remaining.to_string(),
                account: order.account.clone(),
            })
            .collect();

        let mut asks: Vec<_> = self
            .asks
            .iter()
            .filter_map(|Reverse(AskBookOrder(order))| {
                self.id_index
                    .get(&order.id)
                    .filter(|o| o.remaining > Decimal::ZERO).cloned()
            })
            .collect();
        asks.sort_by(|a, b| a.price.cmp(&b.price).then_with(|| a.ts.cmp(&b.ts)));
        let asks = asks
            .into_iter()
            .map(|order| Ask {
                id: order.id.clone(),
                price: order.price.to_string(),
                remaining: order.remaining.to_string(),
                account: order.account.clone(),
            })
            .collect();

        Order {
            pair: self.pair.clone(),
            bids,
            asks,
        }
    }
}

struct MatcherEngine {
    books: HashMap<String, OrderBook>,
}

impl MatcherEngine {
    fn new() -> Self {
        MatcherEngine {
            books: HashMap::new(),
        }
    }

    fn ingest(&mut self, raw: RawOrder) {
        let book = self
            .books
            .entry(raw.pair.clone())
            .or_insert_with(|| OrderBook::new(raw.pair.clone()));
        book.process(raw);
    }

    fn finish(&self) -> (Vec<Order>, Vec<Trade>) {
        let orderbooks = self.books.values().map(|b| b.normalize()).collect();
        let trades = self.books.values().flat_map(|b| b.trades.clone()).collect();
        (orderbooks, trades)
    }
}

fn main() {
    let input_path = "orders.json";
    let orderbook_path = "orderbook.json";
    let trades_path = "trades.json";

    let input = fs::read_to_string(input_path).expect("Failed to read input file");
    let raw_orders: Vec<RawOrder> =
        serde_json::from_str(&input).expect("Failed to parse input JSON");

    let mut engine = MatcherEngine::new();
    for raw in raw_orders {
        engine.ingest(raw);
    }
    let (orderbooks, trades) = engine.finish();

    fs::write(
        orderbook_path,
        serde_json::to_string_pretty(&orderbooks).unwrap(),
    )
    .expect("Failed to write orderbook");
    fs::write(trades_path, serde_json::to_string_pretty(&trades).unwrap())
        .expect("Failed to write trades");
}

// Unit Tests

#[cfg(test)]
mod tests {
    use super::*;

    fn create_raw_order(
        type_op: Operation,
        account_id: &str,
        amount: &str,
        order_id: &str,
        pair: &str,
        limit_price: &str,
        side: Side,
    ) -> RawOrder {
        RawOrder {
            type_op,
            account_id: account_id.to_string(),
            amount: amount.to_string(),
            order_id: order_id.to_string(),
            pair: pair.to_string(),
            limit_price: limit_price.to_string(),
            side,
        }
    }

    // ### Test 1: Adding Buy and Sell Orders
    #[test]
    fn test_add_orders() {
        let mut book = OrderBook::new("BTCUSD".to_string());

        let raw_buy = create_raw_order(
            Operation::CREATE,
            "acc1",
            "10",
            "order1",
            "BTCUSD",
            "100",
            Side::BUY,
        );
        book.process(raw_buy);

        let raw_sell = create_raw_order(
            Operation::CREATE,
            "acc2",
            "10",
            "order2",
            "BTCUSD",
            "101",
            Side::SELL,
        );
        book.process(raw_sell);

        let normalized = book.normalize();
        assert_eq!(normalized.pair, "BTCUSD");
        assert_eq!(normalized.bids.len(), 1, "Should have 1 bid");
        assert_eq!(normalized.asks.len(), 1, "Should have 1 ask");
        assert_eq!(normalized.bids[0].id, "order1");
        assert_eq!(normalized.bids[0].price, "100");
        assert_eq!(normalized.bids[0].remaining, "10");
        assert_eq!(normalized.asks[0].id, "order2");
        assert_eq!(normalized.asks[0].price, "101");
        assert_eq!(normalized.asks[0].remaining, "10");
    }

    // ### Test 2: Deleting an Order
    #[test]
    fn test_delete_order() {
        let mut book = OrderBook::new("BTCUSD".to_string());

        let raw_buy = create_raw_order(
            Operation::CREATE,
            "acc1",
            "10",
            "order1",
            "BTCUSD",
            "100",
            Side::BUY,
        );
        book.process(raw_buy);

        let raw_delete = create_raw_order(
            Operation::DELETE,
            "acc1",
            "0",
            "order1",
            "BTCUSD",
            "0",
            Side::BUY,
        );
        book.process(raw_delete);

        let normalized = book.normalize();
        assert_eq!(normalized.bids.len(), 0, "Bids should be empty");
        assert_eq!(normalized.asks.len(), 0, "Asks should be empty");
    }

    // ### Test 3: Matching Orders (Full Match)
    #[test]
    fn test_match_orders() {
        let mut book = OrderBook::new("BTCUSD".to_string());

        let raw_sell = create_raw_order(
            Operation::CREATE,
            "acc1",
            "10",
            "sell1",
            "BTCUSD",
            "100",
            Side::SELL,
        );
        book.process(raw_sell);

        let raw_buy = create_raw_order(
            Operation::CREATE,
            "acc2",
            "5",
            "buy1",
            "BTCUSD",
            "101",
            Side::BUY,
        );
        book.process(raw_buy);

        let normalized = book.normalize();
        assert_eq!(normalized.bids.len(), 0, "No bids should remain");
        assert_eq!(normalized.asks.len(), 1, "One ask should remain");
        assert_eq!(normalized.asks[0].id, "sell1");
        assert_eq!(normalized.asks[0].remaining, "5");

        assert_eq!(book.trades.len(), 1, "Should have 1 trade");
        let trade = &book.trades[0];
        assert_eq!(trade.pair, "BTCUSD");
        assert_eq!(trade.buy_order_id, "buy1");
        assert_eq!(trade.sell_order_id, "sell1");
        assert_eq!(
            trade.price, "100",
            "Trade price should be the resting ask price"
        );
        assert_eq!(trade.amount, "5");
    }

    // ### Test 4: Partial Matching
    #[test]
    fn test_partial_match() {
        let mut book = OrderBook::new("BTCUSD".to_string());

        let raw_buy = create_raw_order(
            Operation::CREATE,
            "acc1",
            "10",
            "buy1",
            "BTCUSD",
            "100",
            Side::BUY,
        );
        book.process(raw_buy);

        let raw_sell = create_raw_order(
            Operation::CREATE,
            "acc2",
            "5",
            "sell1",
            "BTCUSD",
            "100",
            Side::SELL,
        );
        book.process(raw_sell);

        let normalized = book.normalize();
        assert_eq!(normalized.bids.len(), 1, "One bid should remain");
        assert_eq!(normalized.bids[0].id, "buy1");
        assert_eq!(normalized.bids[0].remaining, "5");
        assert_eq!(normalized.asks.len(), 0, "No asks should remain");

        assert_eq!(book.trades.len(), 1, "Should have 1 trade");
        let trade = &book.trades[0];
        assert_eq!(trade.buy_order_id, "buy1");
        assert_eq!(trade.sell_order_id, "sell1");
        assert_eq!(
            trade.price, "100",
            "Trade price should be the resting bid price"
        );
        assert_eq!(trade.amount, "5");
    }

    // ### Test 5: Multiple Orders at Same Price (Time Priority)
    #[test]
    fn test_multiple_orders_same_price() {
        let mut book = OrderBook::new("BTCUSD".to_string());

        let raw_buy1 = create_raw_order(
            Operation::CREATE,
            "acc1",
            "5",
            "buy1",
            "BTCUSD",
            "100",
            Side::BUY,
        );
        book.process(raw_buy1);

        let raw_buy2 = create_raw_order(
            Operation::CREATE,
            "acc2",
            "5",
            "buy2",
            "BTCUSD",
            "100",
            Side::BUY,
        );
        book.process(raw_buy2);

        let raw_sell = create_raw_order(
            Operation::CREATE,
            "acc3",
            "10",
            "sell1",
            "BTCUSD",
            "100",
            Side::SELL,
        );
        book.process(raw_sell);

        let normalized = book.normalize();
        assert_eq!(normalized.bids.len(), 0, "No bids should remain");
        assert_eq!(normalized.asks.len(), 0, "No asks should remain");

        assert_eq!(book.trades.len(), 2, "Should have 2 trades");

        assert_eq!(book.trades[0].buy_order_id, "buy1");
        assert_eq!(book.trades[0].sell_order_id, "sell1");
        assert_eq!(book.trades[0].amount, "5");
        assert_eq!(book.trades[0].price, "100");

        assert_eq!(book.trades[1].buy_order_id, "buy2");
        assert_eq!(book.trades[1].sell_order_id, "sell1");
        assert_eq!(book.trades[1].amount, "5");
        assert_eq!(book.trades[1].price, "100");
    }

    // ### Test 6: Multiple Trading Pairs
    #[test]
    fn test_multiple_pairs() {
        let mut engine = MatcherEngine::new();

        let raw_btc = create_raw_order(
            Operation::CREATE,
            "acc1",
            "10",
            "order1",
            "BTCUSD",
            "100",
            Side::BUY,
        );
        engine.ingest(raw_btc);

        let raw_eth = create_raw_order(
            Operation::CREATE,
            "acc2",
            "20",
            "order2",
            "ETHUSD",
            "200",
            Side::SELL,
        );
        engine.ingest(raw_eth);

        let (orderbooks, trades) = engine.finish();
        assert_eq!(orderbooks.len(), 2, "Should have 2 order books");
        assert_eq!(trades.len(), 0, "No trades should occur");

        let btc_book = orderbooks.iter().find(|ob| ob.pair == "BTCUSD").unwrap();
        assert_eq!(btc_book.bids.len(), 1, "BTCUSD should have 1 bid");
        assert_eq!(btc_book.bids[0].id, "order1");
        assert_eq!(btc_book.asks.len(), 0);

        let eth_book = orderbooks.iter().find(|ob| ob.pair == "ETHUSD").unwrap();
        assert_eq!(eth_book.bids.len(), 0);
        assert_eq!(eth_book.asks.len(), 1, "ETHUSD should have 1 ask");
        assert_eq!(eth_book.asks[0].id, "order2");
    }
}
