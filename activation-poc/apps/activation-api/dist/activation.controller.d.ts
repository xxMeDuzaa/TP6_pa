import { ActivationService } from './activation.service';
export declare class ActivationController {
    private readonly activationService;
    constructor(activationService: ActivationService);
    createActivation(data: any): Promise<{
        activationId: string;
        status: string;
    }>;
    getActivation(id: string): Promise<(import("mongoose").Document<unknown, {}, import("./schemas/activation.schema").ActivationDocument, {}, import("mongoose").DefaultSchemaOptions> & import("./schemas/activation.schema").Activation & import("mongoose").Document<import("mongoose").Types.ObjectId, any, any, Record<string, any>, {}> & Required<{
        _id: string & import("mongoose").Types.ObjectId;
    }> & {
        __v: number;
    } & {
        id: string;
    }) | null>;
    handleBillingEvents(message: any): Promise<void>;
    handleProvisioningEvents(message: any): Promise<void>;
}
