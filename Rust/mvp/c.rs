use std::collections::{BinaryHeap, HashMap};
use std::cmp::Ordering;
use std::fs;
use serde::{Deserialize, Serialize};
use bigdecimal::BigDecimal;
use std::str::FromStr;
use chrono::Utc;

// Constants
const DEFAULT_INPUT_PATH: &str = "orders.json";
const DEFAULT_ORDER_BOOK_PATH: &str = "orderbook.json";
const DEFAULT_TRADE_BOOK_PATH: &str = "trades.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "UPPERCASE")]
enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Deserialize)]
struct RawOrder {
    type_op: String,
    account_id: String,
    amount: String,
    order_id: String,
    pair: String,
    limit_price: String,
    side: Side,
}

#[derive(Debug, Clone)]
struct BookOrder {
    id: String,
    account: String,
    side: Side,
    pair: String,
    price: BigDecimal,
    remaining: BigDecimal,
    ts: usize, // time-sequence for FIFO
}

#[derive(Debug, Serialize)]
struct Trade {
    pair: String,
    #[serde(rename = "buyOrderId")]
    buy_order_id: String,
    #[serde(rename = "sellOrderId")]
    sell_order_id: String,
    price: String,
    amount: String,
    ts: i64,
}

#[derive(Debug, Serialize)]
struct Order {
    pair: String,
    bids: Vec<OrderEntry>,
    asks: Vec<OrderEntry>,
}

#[derive(Debug, Serialize)]
struct OrderEntry {
    id: String,
    price: String,
    remaining: String,
    account: String,
}

// Custom ordering for BookOrder in heaps
impl PartialEq for BookOrder {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for BookOrder {}

impl PartialOrd for BookOrder {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BookOrder {
    fn cmp(&self, other: &Self) -> Ordering {
        // For bids (max-heap): highest price first, FIFO on equal price
        // For asks (min-heap): lowest price first, FIFO on equal price
        // Note: BinaryHeap is a max-heap, so we need to invert for asks
        match self.side {
            Side::Buy => {
                match self.price.cmp(&other.price) {
                    Ordering::Equal => other.ts.cmp(&self.ts), // Earlier orders have priority
                    ord => ord,
                }
            }
            Side::Sell => {
                match other.price.cmp(&self.price) { // Inverted for min-heap behavior
                    Ordering::Equal => other.ts.cmp(&self.ts), // Earlier orders have priority
                    ord => ord,
                }
            }
        }
    }
}

struct OrderBook {
    pair: String,
    bids: BinaryHeap<BookOrder>,
    asks: BinaryHeap<BookOrder>,
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

    fn process(&mut self, raw: &RawOrder) {
        if raw.type_op == "DELETE" {
            if let Some(found) = self.id_index.get(&raw.order_id).cloned() {
                self.remove(&found);
            }
            return;
        }

        let mut order = BookOrder {
            id: raw.order_id.clone(),
            account: raw.account_id.clone(),
            side: raw.side,
            pair: raw.pair.clone(),
            price: BigDecimal::from_str(&raw.limit_price).unwrap(),
            remaining: BigDecimal::from_str(&raw.amount).unwrap(),
            ts: self.seq,
        };
        self.seq += 1;

        self.match_order(&mut order);
        if order.remaining > BigDecimal::from(0) {
            self.add(order);
        }
    }

    fn add(&mut self, order: BookOrder) {
        self.id_index.insert(order.id.clone(), order.clone());
        match order.side {
            Side::Buy => self.bids.push(order),
            Side::Sell => self.asks.push(order),
        }
    }

    fn remove(&mut self, order: &BookOrder) {
        self.id_index.remove(&order.id);
        
        // Remove from heap by rebuilding without the target order
        match order.side {
            Side::Buy => {
                let orders: Vec<_> = self.bids.drain().filter(|o| o.id != order.id).collect();
                self.bids = orders.into_iter().collect();
            }
            Side::Sell => {
                let orders: Vec<_> = self.asks.drain().filter(|o| o.id != order.id).collect();
                self.asks = orders.into_iter().collect();
            }
        }
    }

    fn match_order(&mut self, incoming: &mut BookOrder) {
        let is_buy = incoming.side == Side::Buy;
        let zero = BigDecimal::from(0);

        while incoming.remaining > zero {
            let mut best = if is_buy {
                match self.asks.peek() {
                    Some(order) => order.clone(),
                    None => break,
                }
            } else {
                match self.bids.peek() {
                    Some(order) => order.clone(),
                    None => break,
                }
            };

            let price_ok = if is_buy {
                incoming.price >= best.price
            } else {
                incoming.price <= best.price
            };

            if !price_ok {
                break;
            }

            let trade_qty = if incoming.remaining < best.remaining {
                incoming.remaining.clone()
            } else {
                best.remaining.clone()
            };
            let trade_price = best.price.clone();

            let trade = Trade {
                pair: self.pair.clone(),
                buy_order_id: if is_buy { incoming.id.clone() } else { best.id.clone() },
                sell_order_id: if is_buy { best.id.clone() } else { incoming.id.clone() },
                price: trade_price.to_string(),
                amount: trade_qty.to_string(),
                ts: Utc::now().timestamp_millis(),
            };
            self.trades.push(trade);

            incoming.remaining = &incoming.remaining - &trade_qty;
            best.remaining = &best.remaining - &trade_qty;

            // Update the best order in the heap
            if is_buy {
                self.asks.pop();
            } else {
                self.bids.pop();
            }

            if best.remaining > zero {
                self.id_index.insert(best.id.clone(), best.clone());
                match best.side {
                    Side::Buy => self.bids.push(best),
                    Side::Sell => self.asks.push(best),
                }
            } else {
                self.id_index.remove(&best.id);
            }
        }
    }

    fn normalize(&self) -> Order {
        let mut bids: Vec<_> = self.bids.iter().cloned().collect();
        let mut asks: Vec<_> = self.asks.iter().cloned().collect();
        
        // Sort to maintain consistent output order
        bids.sort_by(|a, b| b.cmp(a));
        asks.sort_by(|a, b| a.cmp(b));

        Order {
            pair: self.pair.clone(),
            bids: bids.into_iter().map(|o| OrderEntry {
                id: o.id,
                account: o.account,
                price: o.price.to_string(),
                remaining: o.remaining.to_string(),
            }).collect(),
            asks: asks.into_iter().map(|o| OrderEntry {
                id: o.id,
                price: o.price.to_string(),
                remaining: o.remaining.to_string(),
                account: o.account,
            }).collect(),
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
        self.books.entry(pair.to_string())
            .or_insert_with(|| OrderBook::new(pair.to_string()))
    }

    fn ingest(&mut self, raw: &RawOrder) {
        let book = self.book_for(&raw.pair);
        book.process(raw);
    }

    fn finish(self) -> (Vec<Order>, Vec<Trade>) {
        let mut all_trades = Vec::new();
        let mut orderbooks = Vec::new();

        for (_, book) in self.books {
            all_trades.extend(book.trades);
            orderbooks.push(book.normalize());
        }

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
            default_input_path: DEFAULT_INPUT_PATH.to_string(),
            default_order_book_path: DEFAULT_ORDER_BOOK_PATH.to_string(),
            default_trade_book_path: DEFAULT_TRADE_BOOK_PATH.to_string(),
        }
    }

    fn store_result(
        &self,
        orderbooks: &[Order],
        trades: &[Trade],
        order_book_path: Option<&str>,
        trade_book_path: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let order_book_path = order_book_path.unwrap_or(&self.default_order_book_path);
        let trade_book_path = trade_book_path.unwrap_or(&self.default_trade_book_path);

        let orderbooks_json = serde_json::to_string_pretty(&orderbooks)?;
        let trades_json = serde_json::to_string_pretty(&trades)?;

        fs::write(order_book_path, orderbooks_json)?;
        fs::write(trade_book_path, trades_json)?;

        Ok(())
    }

    async fn main(&self, input_path: Option<&str>) -> Result<(Vec<Order>, Vec<Trade>), Box<dyn std::error::Error>> {
        let input_path = input_path.unwrap_or(&self.default_input_path);
        let result = self.process(input_path).await?;
        self.store_result(&result.0, &result.1, None, None)?;
        Ok(result)
    }

    async fn process(&self, input_path: &str) -> Result<(Vec<Order>, Vec<Trade>), Box<dyn std::error::Error>> {
        let content = fs::read_to_string(input_path)?;
        let raw_orders: Vec<RawOrder> = serde_json::from_str(&content)?;

        let mut engine = MatcherEngine::new();
        for raw_order in &raw_orders {
            engine.ingest(raw_order);
        }

        Ok(engine.finish())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = ProcessOrderService::new();
    let (orderbooks, trades) = service.main(None).await?;
    
    println!("Processed {} orderbooks and {} trades", orderbooks.len(), trades.len());
    
    Ok(())
}
