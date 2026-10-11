import { Document } from 'mongoose';
export type ActivationDocument = Activation & Document;
export declare class Activation {
    _id: string;
    customerId: string;
    planId: string;
    status: string;
    simulateFailure: string;
    steps: {
        billing?: {
            status: string;
            at: Date;
        };
        provisioning?: {
            status: string;
            at: Date;
        };
    };
    history: Array<{
        eventType: string;
        at: Date;
    }>;
}
export declare const ActivationSchema: import("mongoose").Schema<Activation, import("mongoose").Model<Activation, any, any, any, any, any, Activation & Required<{
    _id: string;
}> & {
    __v: number;
}>, {}, {}, {}, {}, import("mongoose").DefaultSchemaOptions, Activation, Document<unknown, {}, Activation, {
    id: string;
}, import("mongoose").DefaultSchemaOptions> & Omit<Activation & Required<{
    _id: string;
}> & {
    __v: number;
}, "id"> & import("mongoose").HydratedDocumentOverrides<{
    id: string;
}>, {
    _id?: import("mongoose").SchemaDefinitionProperty<string, Activation, Document<unknown, {}, Activation, {
        id: string;
    }, import("mongoose").DefaultSchemaOptions> & Omit<Activation & Required<{
        _id: string;
    }> & {
        __v: number;
    }, "id"> & import("mongoose").HydratedDocumentOverrides<{
        id: string;
    }>> | undefined;
    customerId?: import("mongoose").SchemaDefinitionProperty<string, Activation, Document<unknown, {}, Activation, {
        id: string;
    }, import("mongoose").DefaultSchemaOptions> & Omit<Activation & Required<{
        _id: string;
    }> & {
        __v: number;
    }, "id"> & import("mongoose").HydratedDocumentOverrides<{
        id: string;
    }>> | undefined;
    planId?: import("mongoose").SchemaDefinitionProperty<string, Activation, Document<unknown, {}, Activation, {
        id: string;
    }, import("mongoose").DefaultSchemaOptions> & Omit<Activation & Required<{
        _id: string;
    }> & {
        __v: number;
    }, "id"> & import("mongoose").HydratedDocumentOverrides<{
        id: string;
    }>> | undefined;
    status?: import("mongoose").SchemaDefinitionProperty<string, Activation, Document<unknown, {}, Activation, {
        id: string;
    }, import("mongoose").DefaultSchemaOptions> & Omit<Activation & Required<{
        _id: string;
    }> & {
        __v: number;
    }, "id"> & import("mongoose").HydratedDocumentOverrides<{
        id: string;
    }>> | undefined;
    simulateFailure?: import("mongoose").SchemaDefinitionProperty<string, Activation, Document<unknown, {}, Activation, {
        id: string;
    }, import("mongoose").DefaultSchemaOptions> & Omit<Activation & Required<{
        _id: string;
    }> & {
        __v: number;
    }, "id"> & import("mongoose").HydratedDocumentOverrides<{
        id: string;
    }>> | undefined;
    steps?: import("mongoose").SchemaDefinitionProperty<{
        billing?: {
            status: string;
            at: Date;
        };
        provisioning?: {
            status: string;
            at: Date;
        };
    }, Activation, Document<unknown, {}, Activation, {
        id: string;
    }, import("mongoose").DefaultSchemaOptions> & Omit<Activation & Required<{
        _id: string;
    }> & {
        __v: number;
    }, "id"> & import("mongoose").HydratedDocumentOverrides<{
        id: string;
    }>> | undefined;
    history?: import("mongoose").SchemaDefinitionProperty<{
        eventType: string;
        at: Date;
    }[], Activation, Document<unknown, {}, Activation, {
        id: string;
    }, import("mongoose").DefaultSchemaOptions> & Omit<Activation & Required<{
        _id: string;
    }> & {
        __v: number;
    }, "id"> & import("mongoose").HydratedDocumentOverrides<{
        id: string;
    }>> | undefined;
}, Activation>;
