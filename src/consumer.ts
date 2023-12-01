import EventEmitter from 'node:events';
import { type Provider, type AwsCredentialIdentity, type Command } from '@smithy/types';
import {
  SQSClient,
  type SQSClientConfig,
  type ReceiveMessageCommandInput,
  type DeleteMessageCommandInput,
  type Message,
  // type ReceiveMessageCommandOutput,
  ReceiveMessageCommand,
  DeleteMessageBatchCommand,
  DeleteMessageBatchRequestEntry,
  // ReceiveMessageResult,
  // DeleteMessageCommand,
  // ReceiveMessageCommandOutput,
} from '@aws-sdk/client-sqs';

export interface PollingConfig {
  intervalSeconds?: number
}

export interface Options {
  ackHandler?: AckHandler,
  ack?: boolean,
  polling?: PollingConfig
  client?: SQSClient
  clientConfig?: SQSClientConfig
  credentialsProvider?: Provider<AwsCredentialIdentity>
  receiveMessageInput?: ReceiveMessageCommandInput
  deleteMessageInput?: DeleteMessageCommandInput
}

export type HandlerReturnType = boolean|undefined|Promise<boolean>|Promise<undefined>
export type HandlerCallbackType = (err: Error, result: boolean) => void;
export type HandlerType = (messages: Message[], callback?: HandlerCallbackType) => HandlerReturnType
export type AckHandler = (messages: Message[]) => void
export type DefaultAckHandler = () => void

function getPollingWithDefaults(config: PollingConfig = {}): PollingConfig {
  const cnf: PollingConfig = {
    intervalSeconds: 5,
  };
  if (Object.hasOwn(config, 'intervalSecs')) {
    cnf.intervalSeconds = config.intervalSeconds;
  }
  return cnf;
}

function isValidWaitTime(n: number): boolean {
  if (n < 0 || n > 20) {
    return false;
  }

  return true;
}

function getReceiveCommandWithDefaults(queue: string, cmd?: ReceiveMessageCommandInput) {
  const command: ReceiveMessageCommandInput = cmd ?? { QueueUrl: queue };
  if (!command.QueueUrl) {
    command.QueueUrl = queue;
  }
  if (!Object.hasOwn(command, 'WaitTimeSeconds') || !isValidWaitTime(command.WaitTimeSeconds ?? -1)) {
    command.WaitTimeSeconds = 20;
  }
  if (!command.MaxNumberOfMessages) {
    command.MaxNumberOfMessages = 10;
  }
  return command;
}

function isPromise<T>(p: unknown): p is Promise<T> {
  return p?.constructor?.name === 'Promise';
}

export class SqsConsumer extends EventEmitter {
  protected queue!: string;

  protected options!: Options;

  protected client!: SQSClient;

  protected receiveMessageInput!: ReceiveMessageCommandInput;

  protected deleteMessageInput?: DeleteMessageCommandInput;

  protected pollingConfig!: PollingConfig;

  protected handler!: HandlerType;

  protected ack!: boolean;

  protected ackHandler!: AckHandler | DefaultAckHandler;

  constructor(queue: string, handler: HandlerType, options: Options) {
    super();
    const {
      ack = true,
      polling,
      client,
      clientConfig,
      credentialsProvider,
      receiveMessageInput,
      deleteMessageInput,
      ackHandler,
    } = options;

    console.log('here we go 1', clientConfig, credentialsProvider);

    if (client) {
      this.client = client;
    } else {
      const cnf = clientConfig ?? { region: 'us-east-1' };

      if (credentialsProvider) {
        cnf.credentials = credentialsProvider;
      }
      this.client = new SQSClient(cnf);
    }

    this.handler = handler;
    this.receiveMessageInput = getReceiveCommandWithDefaults(queue, receiveMessageInput);
    this.deleteMessageInput = deleteMessageInput;
    this.pollingConfig = getPollingWithDefaults(polling);
    this.ack = ack;
    this.ackHandler = ackHandler ?? (() => {});
  }

  public start() {
    this.poll();
    // this is where we will start the timeout, or scheduler, or something to
    // continuosly open the long polling connection.
    // we may way to have a scheduler class that keeps track of the most recent
    // success timestamp, which should start the timer for the next request
  }

  public stop() {

  }

  private deleteMessage(message: Message) {
    console.log('we got a message to delete!!!', message);
    // const input: DeleteMessageCommandInput = this.deleteMessageInput ?? { QueueUrl: this.queue };
    // const command = new DeleteMEssj();
  }

  private handleHandlerResponse(messages: Message[], result: HandlerReturnType): void {
    console.log('we got the result back from the handler!!!', result);

    if (this.ack === true) {
      console.log('ack is true so we will delete the messages from the queue');
    }
    // if (this.ack === true) {
    //   // delete the message from the queue
    //   this.deleteMessage();
    // }
  }

  processHandlerResult(messages: Message[], result: boolean): void {
    // unless the result is explicity false, we'll delete all of the mesages
    if (this.ack === false && result === false) {
      // ack is false, and the result was explicity false, so we wont delete the batch of messages
      return;
    }

    const entries: DeleteMessageBatchRequestEntry[] = [];
    for (let i = 0; i < messages.length; i += 1) {
      const entry: DeleteMessageBatchRequestEntry = {
        Id: `${i}`,
        ReceiptHandle: messages[i].ReceiptHandle,
      };
      entries.push(entry);
    }
    const command = new DeleteMessageBatchCommand({
      QueueUrl: this.queue,
      Entries: entries,
    });

    this.client.send(command, (err, data) => {
      if (err) {
        this.emit('ack-error', err, messages);
        return;
      }
      this.ackHandler(messages);
    });
  }

  processHandlerResponse(response: HandlerReturnType): void {
    // if (!isPromise<booleandd
    if (response === undefined || typeof response === 'boolean') {
      // consumer must call callback for anything to happen
      return;
    }

    if (isPromise<boolean>(response)) {
      response.then((result) => this.processHandlerResult.bind(this));
    }

    //
    // console.log(isPromise<boolean>(response));

    // console.log(typeof response);

    // if ()

    // if (response === undefined || typeof response === 'boolean') {
    //   // the consumer needs to call the callback
    // } else if (response?.constructor?.name === 'Promise') {
    //   // we're dealing with a promise, so the result of the resolved promise is the result we need
    //   // to call the delete method
    //   Promise.resolve(response).then(this.processHandlerResult.bind(this));
    // } else if (typeof response === 'object' && isResultObject(response)) {
    //   // the result is whether or
    // }

    // if (response )
  }

  private handlerCallback(err: Error, result: boolean): void {
    console.log('hello from insdie the callback', err, result);
  }

  async poll() {
    const cmd = new ReceiveMessageCommand(this.receiveMessageInput);
    this.client.send(cmd, (err, data) => {
      if (err) {
        this.emit('receive-error', err);
        return;
      }

      if (data?.Messages) {
        const response = this.handler(data.Messages, this.handlerCallback.bind(this));
        // if it's not a promise, the consumer is expected to call the callback directly
        if (isPromise<boolean>(response)) {
          response.then(this.processHandlerResult.bind(this, data.Messages));
        }
        // this.processHandlerResponse(this.handler(data.Messages, this.handlerCallback.bind(this)));

        // this.handleHandlerResponse(this.handler(data.Messages, this.handlerCallback.bind(this)));
        // const result = this.handler(data.Messages, this.handlerCallback.bind(this));
        // if (result?.constructor?.name === 'Promise') {
        //   Promise.resolve(result).then((this.handleHandlerResponse.bind(this, data.Messages)));
        //   return;
        // } if (result === true || result === false) { // result !== undefined) {
        //   console.log('result is not undefined');
        //   this.handleHandlerResponse(data.Messages, result);
        //   return;
        // }

        console.log('the response was undefined, which means the client has to call the callback');
      }
    });
  }

  // deleteMessages(data: R  {
  //   console.log('were now deleting the thinging', data);
  // }

  // getMessages() {
  //   const cmd = new ReceiveMessageCommand({
  //     QueueUrl: this.queue,
  //     // WaitTimeSeconds:
  //   });
  // }

  // static async init(tmgConfig, configKey = 'sqs') {
  //   return tmgConfig.proxy(
  //     (nc) => new Consumer(nc, { logger: tmgConfig.logger }),
  //     configKey,
  //     ''
  //   )
  //  // const { config } = tmgConfig;
  //
  //  return tmgConfig.proxy(
  //    (nc) => new Consumer(nc, { logger: tmgConfig.logger }),j
  //  )
  //  return tmgConfig.proxy(
  //    (nc) => new Consumer(nc, { logger: tmgConfig.logger }),
  //
  //  )
  //
  // }
}

// const consumer = () => {
//   // const { clientConfig } = config
//
//
//   return {
//     start() {
//
//     },
//     stop() {
//
//     }
//   }
//
// }
