import { ClientKafka } from '@nestjs/microservices';
import { Model } from 'mongoose';
import { Activation, ActivationDocument } from './schemas/activation.schema';
import { ActivationGateway } from './activation.gateway';
export declare class ActivationService {
    private activationModel;
    private kafkaClient;
    private gateway;
    constructor(activationModel: Model<ActivationDocument>, kafkaClient: ClientKafka, gateway: ActivationGateway);
    onModuleInit(): Promise<void>;
    createActivation(data: any): Promise<string>;
    getActivation(id: string): Promise<(import("mongoose").Document<unknown, {}, ActivationDocument, {}, import("mongoose").DefaultSchemaOptions> & Activation & import("mongoose").Document<import("mongoose").Types.ObjectId, any, any, Record<string, any>, {}> & Required<{
        _id: string & import("mongoose").Types.ObjectId;
    }> & {
        __v: number;
    } & {
        id: string;
    }) | null>;
    handleEvent(message: any): Promise<void>;
    private createFinalEvent;
}
