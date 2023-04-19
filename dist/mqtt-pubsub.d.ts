/// <reference types="node" />
import { PubSubEngine } from 'graphql-subscriptions/dist/pubsub-engine';
import { Client, ISubscriptionGrant, IClientPublishOptions, IClientSubscribeOptions } from 'mqtt';
export interface PubSubMQTTOptions {
    brokerUrl?: string;
    client?: Client;
    connectionListener?: (err: Error) => void;
    publishOptions?: PublishOptionsResolver;
    subscribeOptions?: SubscribeOptionsResolver;
    onMQTTSubscribe?: (id: number, granted: ISubscriptionGrant[]) => void;
    triggerTransform?: TriggerTransform;
    parseMessageWithEncoding?: BufferEncoding;
    includeFullPacketInfo?: boolean;
}
export declare class MQTTPubSub implements PubSubEngine {
    private triggerTransform;
    private onMQTTSubscribe;
    private subscribeOptionsResolver;
    private publishOptionsResolver;
    private mqttConnection;
    private subscriptionMap;
    private subsRefsMap;
    private currentSubscriptionId;
    private parseMessageWithEncoding;
    private includeFullPacketInfo?;
    private static matches;
    constructor(options?: PubSubMQTTOptions);
    publish(trigger: string, payload: any): boolean;
    subscribe(trigger: string, onMessage: Function, options?: Object): Promise<number>;
    unsubscribe(subId: number): void;
    asyncIterator<T>(triggers: string | string[]): AsyncIterator<T>;
    private onMessage;
}
export declare type Path = Array<string | number>;
export declare type Trigger = string | Path;
export declare type TriggerTransform = (trigger: Trigger, channelOptions?: Object) => string;
export declare type SubscribeOptionsResolver = (trigger: Trigger, channelOptions?: Object) => Promise<IClientSubscribeOptions>;
export declare type PublishOptionsResolver = (trigger: Trigger, payload: any) => Promise<IClientPublishOptions>;
export declare type SubscribeHandler = (id: number, granted: ISubscriptionGrant[]) => void;
