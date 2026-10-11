import { Controller, Post, Body, Get, Param, HttpCode } from '@nestjs/common';
import { ActivationService } from './activation.service';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS } from '@activation-poc/contracts';

@Controller('activations')
export class ActivationController {
  constructor(private readonly activationService: ActivationService) {}

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
  async handleBillingEvents(@Payload() message: any) {
    await this.activationService.handleEvent(message);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioningEvents(@Payload() message: any) {
    await this.activationService.handleEvent(message);
  }
}
