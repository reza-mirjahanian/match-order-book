import { Test, TestingModule } from '@nestjs/testing';
import { ProcessOrderController } from '../process-order.controller';
import { ProcessOrderService } from '../process-order.service';

describe('ProcessOrderController', () => {
  let controller: ProcessOrderController;
  let service: ProcessOrderService;

  const mockService = {
    runFromFile: jest.fn(),
    runFromJson: jest.fn(),
  };

  beforeEach(async () => {
    const app: TestingModule = await Test.createTestingModule({
      controllers: [ProcessOrderController],
      providers: [ProcessOrderService],
    })
      .overrideProvider(ProcessOrderService)
      .useValue(mockService)
      .compile();

    controller = app.get<ProcessOrderController>(ProcessOrderController);
    service = app.get<ProcessOrderService>(ProcessOrderService);
  });

  it('controller should be defined', () => {
    expect(controller).toBeDefined();
  });

  describe('processFile()', () => {
    beforeEach(() => {
      jest.spyOn(service, 'runFromFile');
      jest.spyOn(service, 'runFromJson');
    });

    it('run() functions should be defined', () => {
      expect(service.runFromFile).toBeDefined();
      expect(service.runFromJson).toBeDefined();
    });

    it('should call service.runFromFile', () => {
      controller.processFile();
      expect(service.runFromFile).toHaveBeenCalledTimes(1);
    });

    it('should call service.runFromJson', () => {
      controller.processJson([
        {
          type_op: 'CREATE',
          account_id: '1',
          amount: '0.00230',
          order_id: '1',
          pair: 'BTC/USDC',
          limit_price: '63500.00',
          side: 'SELL',
        },
      ]);
      expect(service.runFromJson).toHaveBeenCalledTimes(1);
    });
  });
});
