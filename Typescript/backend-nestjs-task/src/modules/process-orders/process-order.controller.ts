import { Body, Controller, Get, Post, ValidationPipe } from '@nestjs/common';
import { ApiBody, ApiOperation, ApiResponse, ApiTags } from '@nestjs/swagger';
import { ProcessOrderService } from './process-order.service';
import { Order, RawOrder, Trade } from './types/trade';

@ApiTags('process-orders')
@Controller('api')
@ApiResponse({ status: 400, description: 'Bad Request.' })
@ApiResponse({ status: 403, description: 'Forbidden.' })
@ApiResponse({ status: 500, description: 'Internal Server Error.' })
export class ProcessOrderController {
  constructor(private readonly processOrderService: ProcessOrderService) {}

  @Get('/process-file')
  @ApiOperation({
    summary: 'Process orders from the file orders.json',
  })
  @ApiResponse({
    status: 200,
    description: 'Operation done successfully.',
  })
  async processFile() {
    return await this.processOrderService.runFromFile();
  }

  @Post('/process-json')
  @ApiOperation({ summary: 'Process orders provided in the request body' })
  @ApiBody({
    schema: {
      type: 'array',
      example: [
        {
          type_op: 'CREATE',
          account_id: '1',
          amount: '0.00230',
          order_id: '1',
          pair: 'BTC/USDC',
          limit_price: '63500.00',
          side: 'SELL',
        },
      ],
    },
  })
  @ApiResponse({ status: 200, description: 'Orderbook & trades returned.' })
  async processJson(
    @Body(new ValidationPipe({ transform: true, whitelist: true }))
    orders: RawOrder[],
  ): Promise<{ orderbooks: Order[]; trades: Trade[] }> {
    if (!Array.isArray(orders) || orders.length === 0) {
      throw new Error('Body must be a non‑empty array of orders');
    }
    return this.processOrderService.runFromJson(orders);
  }
}
