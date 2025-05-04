import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { ProcessOrderModule } from './modules/process-orders/process-order.module';

@Module({
  imports: [ProcessOrderModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
