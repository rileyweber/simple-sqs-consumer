// import { type SQSClientConfigType } from '@aws-sdk/client-sqs';
// import EventEmitter from 'node:events';
// import Consumer from './consumer';
//
// export type ConsumerConfig = {
//   queueName: string,
//
// }
// export type ConsumerConfigs = ConsumerConfig[]
//
// export interface Config {
//   client: SQSClientConfigType
//   consumers: ConsumerConfigs
// }
//
// // export interface ClientConfig {
// //
// // }
//
// export default class Engine extends EventEmitter {
//   protected config!: Config;
//
//   protected clientConfig!: SQSClientConfigType;
//
//   protected consumerConfig!: ConsumerConfigs;
//
//   constructor(config: Config) {
//     super();
//     this.config = config;
//
//     this.clientConfig = config.client;
//     this.consumerConfig = config.consumers;
//   }
//
//   start() {
//     const { consumers } = this.config;
//   }
// }
