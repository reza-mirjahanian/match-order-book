import { Module } from '@nestjs/common';
import { ProcessOrderService } from './process-order.service';
import { ProcessOrderController } from './process-order.controller';

@Module({
  providers: [ProcessOrderService],
  controllers: [ProcessOrderController],
})
export class ProcessOrderModule {}
