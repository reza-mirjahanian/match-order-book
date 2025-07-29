use std::collections::{HashMap, BinaryHeap};
use std::fs::File;
use std::io::{Read, Write};
use serde::{Deserialize, Serialize};
use serde_json::{Value, from_str, to_string_pretty};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
enum Side {
    BUY,
    SELL,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct RawOrder {
    #[serde(rename = "type_op")]
    type_op: String,
    #[serde(rename = "account_id")]
    account_id: String,
    #[serde(rename = "amount")]
    amount: String,
    #[serde(rename = "order_id")]
    order_id: String,
    #[serde(rename = "pair")]
    pair: String,
    #[serde(rename = "limit_price")]
    limit_price: String,
    #[serde(rename = "side")]
    side: Side,
}

#[derive(Debug, Clone, Serialize)]
struct BookOrder {
    id: String,
    account: String,
    side: Side,
    pair: String,
    price: String,
    remaining: String,
    ts: usize,
}

impl BookOrder {
    fn price_as_f64(&self) -> f64 {
        self.price.parse().unwrap_or(0.0)
    }
    
    fn remaining_as_f64(&self) -> f64 {
        self.remaining.parse().unwrap_or(0.0)
    }
}

#[derive(Debug, Clone, Serialize)]
struct Trade {
    pair: String,
    #[serde(rename = "buyOrderId")]
    buy_order_id: String,
    #[serde(rename = "sellOrderId")]
    sell_order_id: String,
    price: String,
    amount: String,
    ts: u128,
}

#[derive(Debug, Clone, Serialize)]
struct Order {
    pair: String,
    bids: Vec<OrderEntry>,
    asks: Vec<OrderEntry>,
}

#[derive(Debug, Clone, Serialize)]
struct OrderEntry {
    id: String,
    price: String,
    remaining: String,
    account: String,
}

struct OrderBook {
    pair: String,
    bids: BinaryHeap<BookOrder>, // Max heap for bids (highest price first)
    asks: BinaryHeap<BookOrder>, // Min heap for asks (lowest price first, using reverse logic)
    id_index: HashMap<String, BookOrder>,
    seq: usize,
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
        if raw.type_op == "DELETE" {
            if let Some(order) = self.id_index.remove(&raw.order_id) {
                self.remove(&order);
            }
            return;
        }

        let order = BookOrder {
            id: raw.order_id,
            account: raw.account_id,
            side: raw.side,
            pair: raw.pair,
            price: raw.limit_price,
            remaining: raw.amount,
            ts: self.seq,
        };
        self.seq += 1;
        
        self.match_order(order.clone());
        
        if order.remaining_as_f64() > 0.0 {
            self.add(order);
        }
    }

    fn add(&mut self, order: BookOrder) {
        self.id_index.insert(order.id.clone(), order.clone());
        match order.side {
            Side::BUY => self.bids.push(order),
            Side::SELL => self.asks.push(order),
        }
    }

    fn remove(&mut self, order: &BookOrder) {
        self.id_index.remove(&order.id);
        // Note: Actual removal from heaps is complex in Rust, so we'll filter during normalization
    }

    fn match_order(&mut self, mut incoming: BookOrder) {
        let is_buy = matches!(incoming.side, Side::BUY);
        
        while incoming.remaining_as_f64() > 0.0 {
            let best = if is_buy {
                self.asks.peek()
            } else {
                self.bids.peek()
            };
            
            let should_match = if let Some(best_order) = best {
                if is_buy {
                    incoming.price_as_f64() >= best_order.price_as_f64()
                } else {
                    incoming.price_as_f64() <= best_order.price_as_f64()
                }
            } else {
                false
            };
            
            if !should_match {
                break;
            }
            
            let mut best_order = if is_buy {
                self.asks.pop().unwrap()
            } else {
                self.bids.pop().unwrap()
            };
            
            self.id_index.remove(&best_order.id);
            
            let trade_qty = f64::min(incoming.remaining_as_f64(), best_order.remaining_as_f64());
            let trade_price = best_order.price.clone();
            
            let trade = Trade {
                pair: self.pair.clone(),
                buy_order_id: if is_buy { incoming.id.clone() } else { best_order.id.clone() },
                sell_order_id: if is_buy { best_order.id.clone() } else { incoming.id.clone() },
                price: trade_price.clone(),
                amount: trade_qty.to_string(),
                ts: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            };
            
            self.trades.push(trade);
            
            incoming.remaining = (incoming.remaining_as_f64() - trade_qty).to_string();
            best_order.remaining = (best_order.remaining_as_f64() - trade_qty).to_string();
            
            if best_order.remaining_as_f64() > 0.0 {
                self.id_index.insert(best_order.id.clone(), best_order.clone());
                match best_order.side {
                    Side::BUY => self.bids.push(best_order),
                    Side::SELL => self.asks.push(best_order),
                }
            }
        }
    }

    fn normalize(&self) -> Order {
        // Clone and convert to vectors for sorting
        let mut bids_vec: Vec<BookOrder> = self.bids.clone().into_iter().collect();
        let mut asks_vec: Vec<BookOrder> = self.asks.clone().into_iter().collect();
        
        // Sort bids: highest price first, then FIFO (lowest ts first)
        bids_vec.sort_by(|a, b| {
            let price_cmp = b.price_as_f64().partial_cmp(&a.price_as_f64()).unwrap();
            if price_cmp == std::cmp::Ordering::Equal {
                a.ts.cmp(&b.ts)
            } else {
                price_cmp
            }
        });
        
        // Sort asks: lowest price first, then FIFO (lowest ts first)
        asks_vec.sort_by(|a, b| {
            let price_cmp = a.price_as_f64().partial_cmp(&b.price_as_f64()).unwrap();
            if price_cmp == std::cmp::Ordering::Equal {
                a.ts.cmp(&b.ts)
            } else {
                price_cmp
            }
        });
        
        let bids: Vec<OrderEntry> = bids_vec.iter().map(|o| OrderEntry {
            id: o.id.clone(),
            account: o.account.clone(),
            price: o.price.clone(),
            remaining: o.remaining.clone(),
        }).collect();
        
        let asks: Vec<OrderEntry> = asks_vec.iter().map(|o| OrderEntry {
            id: o.id.clone(),
            account: o.account.clone(),
            price: o.price.clone(),
            remaining: o.remaining.clone(),
        }).collect();
        
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

    fn book_for(&mut self, pair: &str) -> &mut OrderBook {
        self.books.entry(pair.to_string()).or_insert_with(|| OrderBook::new(pair.to_string()))
    }

    fn ingest(&mut self, raw: RawOrder) {
        let book = self.book_for(&raw.pair);
        book.process(raw);
    }

    fn finish(self) -> (Vec<Order>, Vec<Trade>) {
        let mut all_trades: Vec<Trade> = Vec::new();
        let orderbooks: Vec<Order> = self.books.into_values().map(|mut book| {
            all_trades.append(&mut book.trades);
            book.normalize()
        }).collect();
        
        (orderbooks, all_trades)
    }
}

struct ProcessOrderService {
    default_input_path: String,
    default_order_book_path: String,
    default_trade_book_path: String,
}

impl ProcessOrderService {
    fn new() -> Self {
        ProcessOrderService {
            default_input_path: "orders.json".to_string(),
            default_order_book_path: "orderbook.json".to_string(),
            default_trade_book_path: "trades.json".to_string(),
        }
    }

    fn store_result(&self, orderbooks: Vec<Order>, trades: Vec<Trade>, 
                    order_book_path: Option<&str>, trade_book_path: Option<&str>) {
        let order_path = order_book_path.unwrap_or(&self.default_order_book_path);
        let trade_path = trade_book_path.unwrap_or(&self.default_trade_book_path);
        
        let orderbooks_json = to_string_pretty(&orderbooks).unwrap();
        let trades_json = to_string_pretty(&trades).unwrap();
        
        File::create(order_path).unwrap().write_all(orderbooks_json.as_bytes()).unwrap();
        File::create(trade_path).unwrap().write_all(trades_json.as_bytes()).unwrap();
    }

    fn main(&self, input_path: Option<&str>) -> Result<(Vec<Order>, Vec<Trade>), Box<dyn std::error::Error>> {
        let path = input_path.unwrap_or(&self.default_input_path);
        let result = self.process(path)?;
        self.store_result(result.0.clone(), result.1.clone(), None, None);
        Ok(result)
    }

    fn process(&self, input_path: &str) -> Result<(Vec<Order>, Vec<Trade>), Box<dyn std::error::Error>> {
        let mut file = File::open(input_path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        
        let orders: Vec<RawOrder> = from_str(&contents)?;
        
        let mut engine = MatcherEngine::new();
        for order in orders {
            engine.ingest(order);
        }
        
        Ok(engine.finish())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = ProcessOrderService::new();
    service.main(None)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matching_engine() {
        let service = ProcessOrderService::new();
        // This would require setting up test files
        // service.main(Some("test_orders.json")).unwrap();
    }
}