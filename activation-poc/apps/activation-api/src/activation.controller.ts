import { Controller, Post, Body, Get, Param, HttpCode, Inject } from '@nestjs/common';
import { ActivationService } from './activation.service';
import { ClientKafka, EventPattern, Payload, Ctx } from '@nestjs/microservices';
import { TOPICS } from '@activation-poc/contracts';
import { RetryWithDlq } from '@activation-poc/kafka-toolkit';

@Controller('activations')
export class ActivationController {
  constructor(
    private readonly activationService: ActivationService,
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka, // usado por @RetryWithDlq
  ) {}

  @Post()
  @HttpCode(202)
  async createActivation(@Body() data: any) {
    const activationId = await this.activationService.createActivation(data);
    return { activationId, status: 'PENDING' };
  }

  @Get(':id')
  async getActivation(@Param('id') id: string) {
    return this.activationService.getActivation(id);
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  @RetryWithDlq(TOPICS.BILLING_EVENTS)
  async handleBillingEvents(@Payload() message: any, @Ctx() _context: unknown) {
    await this.activationService.handleEvent(message);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  @RetryWithDlq(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioningEvents(@Payload() message: any, @Ctx() _context: unknown) {
    await this.activationService.handleEvent(message);
  }
}
