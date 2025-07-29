use serde::{Deserialize, Serialize};
use std::collections::{BinaryHeap, HashMap};
use std::cmp::Ordering;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RawOrder {
    pub type_op: String, // "CREATE" or "DELETE"
    pub account_id: String,
    pub amount: String,
    pub order_id: String,
    pub pair: String,
    pub limit_price: String,
    pub side: Side,
}

#[derive(Debug, Clone)]
pub struct BookOrder {
    pub id: String,
    pub account: String,
    pub side: Side,
    pub pair: String,
    pub price: Decimal,
    pub remaining: Decimal,
    pub ts: u64, // time-sequence for FIFO
}

// Wrapper for BookOrder to implement custom ordering for BinaryHeap
#[derive(Debug, Clone)]
struct BidOrder(BookOrder);
#[derive(Debug, Clone)]
struct AskOrder(BookOrder);

impl PartialEq for BidOrder {
    fn eq(&self, other: &Self) -> bool {
        self.0.price == other.0.price && self.0.ts == other.0.ts
    }
}

impl Eq for BidOrder {}

impl PartialOrd for BidOrder {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BidOrder {
    fn cmp(&self, other: &Self) -> Ordering {
        // Bid max-heap: highest price first, FIFO on equal price
        match self.0.price.cmp(&other.0.price) {
            Ordering::Equal => other.0.ts.cmp(&self.0.ts), // FIFO: earlier timestamp first
            other => other, // Higher price first (reverse for max-heap)
        }
    }
}

impl PartialEq for AskOrder {
    fn eq(&self, other: &Self) -> bool {
        self.0.price == other.0.price && self.0.ts == other.0.ts
    }
}

impl Eq for AskOrder {}

impl PartialOrd for AskOrder {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AskOrder {
    fn cmp(&self, other: &Self) -> Ordering {
        // Ask min-heap: lowest price first, FIFO on equal price
        match self.0.price.cmp(&other.0.price) {
            Ordering::Equal => other.0.ts.cmp(&self.0.ts), // FIFO: earlier timestamp first
            other => other.reverse(), // Lower price first (reverse for min-heap behavior)
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Trade {
    pub pair: String,
    #[serde(rename = "buyOrderId")]
    pub buy_order_id: String,
    #[serde(rename = "sellOrderId")]
    pub sell_order_id: String,
    pub price: String,
    pub amount: String,
    pub ts: u64,
}

#[derive(Debug, Serialize)]
pub struct OrderBookEntry {
    pub id: String,
    pub price: String,
    pub remaining: String,
    pub account: String,
}

#[derive(Debug, Serialize)]
pub struct Order {
    pub pair: String,
    pub bids: Vec<OrderBookEntry>,
    pub asks: Vec<OrderBookEntry>,
}

pub struct OrderBook {
    pair: String,
    bids: BinaryHeap<BidOrder>,
    asks: BinaryHeap<AskOrder>,
    id_index: HashMap<String, BookOrder>,
    seq: u64,
    pub trades: Vec<Trade>,
}

impl OrderBook {
    pub fn new(pair: String) -> Self {
        Self {
            pair,
            bids: BinaryHeap::new(),
            asks: BinaryHeap::new(),
            id_index: HashMap::new(),
            seq: 0,
            trades: Vec::new(),
        }
    }

    pub fn process(&mut self, raw: &RawOrder) {
        if raw.type_op == "DELETE" {
            if let Some(found) = self.id_index.get(&raw.order_id).cloned() {
                self.remove(&found);
            }
            return;
        }

        let order = BookOrder {
            id: raw.order_id.clone(),
            account: raw.account_id.clone(),
            side: raw.side.clone(),
            pair: raw.pair.clone(),
            price: Decimal::from_str(&raw.limit_price).unwrap_or_default(),
            remaining: Decimal::from_str(&raw.amount).unwrap_or_default(),
            ts: self.seq,
        };
        self.seq += 1;

        let mut order = order;
        self.match_order(&mut order);
        
        if order.remaining > Decimal::ZERO {
            self.add(order);
        }
    }

    fn add(&mut self, order: BookOrder) {
        self.id_index.insert(order.id.clone(), order.clone());
        match order.side {
            Side::Buy => self.bids.push(BidOrder(order)),
            Side::Sell => self.asks.push(AskOrder(order)),
        }
    }

    fn remove(&mut self, order: &BookOrder) {
        self.id_index.remove(&order.id);
        // Note: BinaryHeap doesn't have efficient remove, so we'll mark as removed
        // and filter during normalization. For a production system, consider using
        // a different data structure that supports efficient removal.
    }

    fn match_order(&mut self, incoming: &mut BookOrder) {
        let is_buy = matches!(incoming.side, Side::Buy);
        
        loop {
            if incoming.remaining <= Decimal::ZERO {
                break;
            }

            let best_match = if is_buy {
                // For buy orders, match against asks (sells)
                self.asks.peek().cloned()
            } else {
                // For sell orders, match against bids (buys)  
                self.bids.peek().cloned()
            };

            let best = match best_match {
                Some(order) => if is_buy { order.0 } else { self.bids.peek().unwrap().0.clone() },
                None => break,
            };

            // Check if we still have this order (not removed)
            if !self.id_index.contains_key(&best.id) {
                // Remove from heap and continue
                if is_buy {
                    self.asks.pop();
                } else {
                    self.bids.pop();
                }
                continue;
            }

            let price_ok = if is_buy {
                incoming.price >= best.price
            } else {
                incoming.price <= best.price
            };

            if !price_ok {
                break;
            }

            let trade_qty = if incoming.remaining < best.remaining {
                incoming.remaining
            } else {
                best.remaining
            };

            let trade_price = best.price;
            let trade = Trade {
                pair: self.pair.clone(),
                buy_order_id: if is_buy { incoming.id.clone() } else { best.id.clone() },
                sell_order_id: if is_buy { best.id.clone() } else { incoming.id.clone() },
                price: trade_price.to_string(),
                amount: trade_qty.to_string(),
                ts: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            };

            self.trades.push(trade);

            incoming.remaining -= trade_qty;
            
            // Update the best order in our index
            if let Some(mut best_order) = self.id_index.get_mut(&best.id) {
                best_order.remaining -= trade_qty;
                
                if best_order.remaining <= Decimal::ZERO {
                    self.remove(&best_order.clone());
                }
            }

            // Remove from heap
            if is_buy {
                self.asks.pop();
            } else {
                self.bids.pop();
            }
        }
    }

    pub fn normalize(&self) -> Order {
        // Filter out removed orders and convert to OrderBookEntry
        let bids: Vec<OrderBookEntry> = self.bids
            .iter()
            .filter(|bid| self.id_index.contains_key(&bid.0.id))
            .map(|bid| OrderBookEntry {
                id: bid.0.id.clone(),
                account: bid.0.account.clone(),
                price: bid.0.price.to_string(),
                remaining: bid.0.remaining.to_string(),
            })
            .collect();

        let asks: Vec<OrderBookEntry> = self.asks
            .iter()
            .filter(|ask| self.id_index.contains_key(&ask.0.id))
            .map(|ask| OrderBookEntry {
                id: ask.0.id.clone(),
                account: ask.0.account.clone(),
                price: ask.0.price.to_string(),
                remaining: ask.0.remaining.to_string(),
            })
            .collect();

        Order {
            pair: self.pair.clone(),
            bids,
            asks,
        }
    }
}

pub struct MatcherEngine {
    books: HashMap<String, OrderBook>,
    trades: Vec<Trade>,
}

impl MatcherEngine {
    pub fn new() -> Self {
        Self {
            books: HashMap::new(),
            trades: Vec::new(),
        }
    }

    fn book_for(&mut self, pair: &str) -> &mut OrderBook {
        self.books
            .entry(pair.to_string())
            .or_insert_with(|| OrderBook::new(pair.to_string()))
    }

    pub fn ingest(&mut self, raw: &RawOrder) {
        let book = self.book_for(&raw.pair);
        book.process(raw);
    }

    pub fn finish(mut self) -> (Vec<Order>, Vec<Trade>) {
        self.trades = self.books
            .values()
            .flat_map(|book| book.trades.clone())
            .collect();

        let orderbooks = self.books
            .into_values()
            .map(|book| book.normalize())
            .collect();

        (orderbooks, self.trades)
    }
}

pub struct ProcessOrderService {
    default_input_path: String,
    default_order_book_path: String,
    default_trade_book_path: String,
}

impl ProcessOrderService {
    pub fn new() -> Self {
        Self {
            default_input_path: "orders.json".to_string(),
            default_order_book_path: "orderbook.json".to_string(),
            default_trade_book_path: "trades.json".to_string(),
        }
    }

    pub fn store_result(
        &self,
        orderbooks: &[Order],
        trades: &[Trade],
        order_book_path: Option<&str>,
        trade_book_path: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let order_book_path = order_book_path.unwrap_or(&self.default_order_book_path);
        let trade_book_path = trade_book_path.unwrap_or(&self.default_trade_book_path);

        let orderbook_json = serde_json::to_string_pretty(orderbooks)?;
        let trades_json = serde_json::to_string_pretty(trades)?;

        fs::write(order_book_path, orderbook_json)?;
        fs::write(trade_book_path, trades_json)?;

        Ok(())
    }

    pub fn main(&self, input_path: Option<&str>) -> Result<(Vec<Order>, Vec<Trade>), Box<dyn std::error::Error>> {
        let input_path = input_path.unwrap_or(&self.default_input_path);
        let result = self.process(input_path)?;
        self.store_result(&result.0, &result.1, None, None)?;
        Ok(result)
    }

    pub fn process(&self, input_path: &str) -> Result<(Vec<Order>, Vec<Trade>), Box<dyn std::error::Error>> {
        let content = fs::read_to_string(input_path)?;
        let orders: Vec<RawOrder> = serde_json::from_str(&content)?;

        let mut engine = MatcherEngine::new();
        
        for order in &orders {
            engine.ingest(order);
        }

        Ok(engine.finish())
    }
}

// Add to Cargo.toml:
// [dependencies]
// serde = { version = "1.0", features = ["derive"] }
// serde_json = "1.0"
// rust_decimal = { version = "1.0", features = ["serde"] }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_matching() {
        let service = ProcessOrderService::new();
        // Add your test cases here
    }
}

// Example main function
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = ProcessOrderService::new();
    let (orderbooks, trades) = service.main(None)?;
    
    println!("Processed {} orderbooks", orderbooks.len());
    println!("Executed {} trades", trades.len());
    
    Ok(())
}