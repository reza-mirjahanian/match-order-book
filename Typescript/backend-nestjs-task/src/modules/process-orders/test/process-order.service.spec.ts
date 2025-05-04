import { Test, TestingModule } from '@nestjs/testing';
import { ProcessOrderService } from '../process-order.service';
import { Order, RawOrder, Trade } from '../types/trade';
import { Readable } from 'stream';

describe('ProcessOrderService', () => {
  let service: ProcessOrderService;

  beforeEach(async () => {
    const app: TestingModule = await Test.createTestingModule({
      providers: [ProcessOrderService],
    }).compile();

    service = app.get<ProcessOrderService>(ProcessOrderService);
  });

  describe('process()', () => {
    it('should produce the expected orderbook and trades for the sample input', async () => {
      // @todo refactor to fixture file
      const orders: RawOrder[] = [
        {
          type_op: 'CREATE',
          account_id: '1',
          amount: '0.00230',
          order_id: '1',
          pair: 'BTC/USDC',
          limit_price: '63500.00',
          side: 'SELL',
        },
        {
          type_op: 'CREATE',
          account_id: '2',
          amount: '0.00230',
          order_id: '2',
          pair: 'BTC/USDC',
          limit_price: '63500.00',
          side: 'BUY',
        },
        {
          type_op: 'CREATE',
          account_id: '1',
          amount: '0.00798',
          order_id: '3',
          pair: 'BTC/USDC',
          limit_price: '62880.54',
          side: 'BUY',
        },
        {
          type_op: 'CREATE',
          account_id: '2',
          amount: '0.00798',
          order_id: '4',
          pair: 'BTC/USDC',
          limit_price: '62880.54',
          side: 'SELL',
        },
        {
          type_op: 'CREATE',
          account_id: '1',
          amount: '0.12785',
          order_id: '5',
          pair: 'BTC/USDC',
          limit_price: '61577.30',
          side: 'SELL',
        },
        {
          type_op: 'DELETE',
          account_id: '1',
          amount: '0.12785',
          order_id: '5',
          pair: 'BTC/USDC',
          limit_price: '61577.30',
          side: 'SELL',
        },
        {
          type_op: 'CREATE',
          account_id: '1',
          amount: '0.20000',
          order_id: '6',
          pair: 'BTC/USDC',
          limit_price: '47500',
          side: 'SELL',
        },
        {
          type_op: 'CREATE',
          account_id: '1',
          amount: '0.20000',
          order_id: '7',
          pair: 'BTC/USDC',
          limit_price: '50500',
          side: 'BUY',
        },
        {
          type_op: 'CREATE',
          account_id: '1',
          amount: '6.34500',
          order_id: '8',
          pair: 'BTC/USDC',
          limit_price: '61577.30',
          side: 'SELL',
        },
        {
          type_op: 'CREATE',
          account_id: '2',
          amount: '2.34500',
          order_id: '9',
          pair: 'BTC/USDC',
          limit_price: '62577.30',
          side: 'BUY',
        },
        {
          type_op: 'CREATE',
          account_id: '2',
          amount: '2.00000',
          order_id: '10',
          pair: 'BTC/USDC',
          limit_price: '63477.30',
          side: 'BUY',
        },
        {
          type_op: 'CREATE',
          account_id: '2',
          amount: '0.50000',
          order_id: '11',
          pair: 'BTC/USDC',
          limit_price: '66577.30',
          side: 'BUY',
        },
        {
          type_op: 'CREATE',
          account_id: '2',
          amount: '3.50000',
          order_id: '12',
          pair: 'BTC/USDC',
          limit_price: '61577.30',
          side: 'BUY',
        },
        {
          type_op: 'CREATE',
          account_id: '2',
          amount: '4.50000',
          order_id: '13',
          pair: 'BTC/USDC',
          limit_price: '62877.30',
          side: 'BUY',
        },
        {
          type_op: 'CREATE',
          account_id: '2',
          amount: '3.50000',
          order_id: '14',
          pair: 'BTC/USDC',
          limit_price: '62877.30',
          side: 'BUY',
        },
        {
          type_op: 'CREATE',
          account_id: '2',
          amount: '1.57600',
          order_id: '15',
          pair: 'BTC/USDC',
          limit_price: '60577.30',
          side: 'BUY',
        },
        {
          type_op: 'CREATE',
          account_id: '2',
          amount: '1.58900',
          order_id: '16',
          pair: 'BTC/USDC',
          limit_price: '65860.30',
          side: 'SELL',
        },
        {
          type_op: 'CREATE',
          account_id: '2',
          amount: '2.67600',
          order_id: '17',
          pair: 'BTC/USDC',
          limit_price: '66490.50',
          side: 'SELL',
        },
        {
          type_op: 'CREATE',
          account_id: '2',
          amount: '0.47600',
          order_id: '18',
          pair: 'BTC/USDC',
          limit_price: '60577.30',
          side: 'BUY',
        },
        {
          type_op: 'CREATE',
          account_id: '2',
          amount: '1.00000',
          order_id: '19',
          pair: 'BTC/USDC',
          limit_price: '60577.30',
          side: 'BUY',
        },
      ];
      const json = JSON.stringify(orders);
      const inputStream = Readable.from([json]);

      const { orderbooks, trades } = await (service as any).process(
        inputStream,
      );

      const expectedOrderbooks: Order[] = [
        {
          pair: 'BTC/USDC',
          bids: [
            { id: '13', account: '2', price: '62877.3', remaining: '4.5' },
            { id: '12', account: '2', price: '61577.3', remaining: '2' },
            { id: '14', account: '2', price: '62877.3', remaining: '3.5' },
            { id: '15', account: '2', price: '60577.3', remaining: '1.576' },
            { id: '18', account: '2', price: '60577.3', remaining: '0.476' },
            { id: '19', account: '2', price: '60577.3', remaining: '1' },
          ],
          asks: [
            { id: '16', account: '2', price: '65860.3', remaining: '1.589' },
            { id: '17', account: '2', price: '66490.5', remaining: '2.676' },
          ],
        },
      ];
      expect(orderbooks).toEqual(expectedOrderbooks);

      const expectedTrades: Omit<Trade, 'ts'>[] = [
        {
          pair: 'BTC/USDC',
          buyOrderId: '2',
          sellOrderId: '1',
          price: '63500',
          amount: '0.0023',
        },
        {
          pair: 'BTC/USDC',
          buyOrderId: '3',
          sellOrderId: '4',
          price: '62880.54',
          amount: '0.00798',
        },
        {
          pair: 'BTC/USDC',
          buyOrderId: '7',
          sellOrderId: '6',
          price: '47500',
          amount: '0.2',
        },
        {
          pair: 'BTC/USDC',
          buyOrderId: '9',
          sellOrderId: '8',
          price: '61577.3',
          amount: '2.345',
        },
        {
          pair: 'BTC/USDC',
          buyOrderId: '10',
          sellOrderId: '8',
          price: '61577.3',
          amount: '2',
        },
        {
          pair: 'BTC/USDC',
          buyOrderId: '11',
          sellOrderId: '8',
          price: '61577.3',
          amount: '0.5',
        },
        {
          pair: 'BTC/USDC',
          buyOrderId: '12',
          sellOrderId: '8',
          price: '61577.3',
          amount: '1.5',
        },
      ];

      expect(trades).toHaveLength(expectedTrades.length);
      expectedTrades.forEach((exp, i) => {
        expect(trades[i]).toMatchObject(exp);
        expect(typeof trades[i].ts).toBe('number');
      });
    });
  });
});
