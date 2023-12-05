import EventEmitter from 'node:events';
import { setTimeout as schedule, clearTimeout } from 'node:timers';
import { type Provider, type AwsCredentialIdentity } from '@smithy/types';
import {
  SQSClient,
  type SQSClientConfig,
  type ReceiveMessageCommandInput,
  type DeleteMessageCommandInput,
  type Message,
  ReceiveMessageCommand,
  DeleteMessageBatchCommand,
  DeleteMessageBatchRequestEntry,
} from '@aws-sdk/client-sqs';

type Timeout = ReturnType<typeof schedule>;

type Done = () => void;
export interface PollingConfig {
  intervalSeconds?: number
}

export type HandlerReturnType = boolean|undefined|Promise<boolean>|Promise<undefined>
export type HandlerCallbackType = (err: Error, result: boolean[]) => void;
export type HandlerType = (messages: Message[], callback?: HandlerCallbackType) => HandlerReturnType
export type AckHandler = (messages: Message[]) => void
export type DefaultAckHandler = () => void
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

function isBoolean(b: unknown): b is boolean {
  return b === true || b === false;
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

  protected running = false;

  private timeout!: Timeout;

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
    this.ackHandler = ackHandler ?? (() => {
      // intentionally do nothing
    });
  }

  public start() {
    if (this.running) {
      return;
    }

    const loop = () => {
      this.poll(() => {
        if (this.timeout) {
          clearTimeout(this.timeout);
        }
        this.timeout = schedule(loop, 0);
      });
    };
    this.running = true;
    loop();
  }

  public stop() {
    if (!this.running) {
      return;
    }
    clearTimeout(this.timeout);
    this.running = false;
  }

  processHandlerResult(messages: Message[], result: boolean[], done: Done): void {
    const entries: DeleteMessageBatchRequestEntry[] = [];
    for (let i = 0; i < result.length; i += 1) {
      const ack = result[i];
      if (this.ack === true || ack === true) {
        const entry: DeleteMessageBatchRequestEntry = {
          Id: `${i}`,
          ReceiptHandle: messages[i].ReceiptHandle,
        };
        entries.push(entry);
      }
    }

    if (entries.length) {
      this.deleteMessages(entries, done);
    } else {
      done();
    }
  }

  private deleteMessages(entries: DeleteMessageBatchRequestEntry[], done: Done, depth = 0): void {
    if (depth >= 4) {
      done();
      return;
    }
    this.client.send(
      new DeleteMessageBatchCommand({ QueueUrl: this.queue, Entries: entries }),
      (err) => {
        if (err) {
          return this.deleteMessages(entries, done, depth + 1);
        }
        return done();
      },
    );
  }

  poll(done: Done) {
    const cmd = new ReceiveMessageCommand(this.receiveMessageInput);
    this.client.send(cmd, (err, data) => {
      if (err) {
        this.emit('receive-error', err);
        done();
        return;
      }

      if (data?.Messages) {
        const response = this.handler(data.Messages, (error, result) => {
          if (error) {
            // errors indicate that we shouldn't delete the messages from the queue
            // these will eventually get cycled back into the queue and retried by this
            // or another consumer
            done();
            return;
          }
          this.processHandlerResult(data.Messages ?? [], result, done);
        });

        // if it's not a promise, the consumer is expected to call the callback directly
        if (isPromise<boolean[]>(response)) {
          response.then((result) => {
            if (Array.isArray(result) && result.every((v) => isBoolean(v))) {
              // this is the only valid result
              this.processHandlerResult(data.Messages ?? [], result, done);
            } else {
              // fail hard here, because the function either needs to return a promise that
              // resolves to a valid result, or no promise at all and should instead invoke the
              // callback.
              throw new TypeError('handler returned a promise that did not resolve to a valid result. Did you mean to call the callback?');
            }
            // in this situation, the callback is expected to be called
          });
        }
      } else {
        // no data was received with the WaitTime. Calling the done function will ensure we
        // poll again for more messages
        done();
      }
    });
  }
}
