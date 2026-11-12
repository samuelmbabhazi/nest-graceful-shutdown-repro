/**
 * A sketch of the proposed behaviour on top of the published 12.5.0 servers:
 * stop taking messages, wait for the handlers in flight, then close.
 * In the framework the counting would live in `Server` itself.
 */
import { BaseRpcContext, ServerRedis, ServerRMQ } from '@nestjs/microservices';
import { sleep } from '../timeline.js';

type ProcessingFn = (runEndHook: () => void) => Promise<void>;

class HandlersInFlight {
  private count = 0;

  /** Wraps the function `Server#runWithProcessingHooks` receives and counts it until it settles. */
  track(fn: ProcessingFn): ProcessingFn {
    this.count++;
    let settled = false;
    const settle = () => {
      if (!settled) {
        settled = true;
        this.count--;
      }
    };
    return runEndHook =>
      fn(() => {
        runEndHook();
        settle();
      }).catch(err => {
        settle();
        throw err;
      });
  }

  async settled(gracePeriod = 10_000) {
    const deadline = Date.now() + gracePeriod;
    while (this.count > 0 && Date.now() < deadline) {
      await sleep(10);
    }
    // The last reply of a request is published on a later tick.
    await sleep(20);
  }
}

export class DrainingServerRedis extends ServerRedis {
  private readonly handlersInFlight = new HandlersInFlight();

  protected runWithProcessingHooks(
    context: BaseRpcContext,
    fn: ProcessingFn,
    endHookContext?: BaseRpcContext,
  ) {
    return super.runWithProcessingHooks(
      context,
      this.handlersInFlight.track(fn),
      endHookContext,
    );
  }

  async close() {
    await this.subClient.unsubscribe();
    await this.handlersInFlight.settled();
    return super.close();
  }
}

export class DrainingServerRMQ extends ServerRMQ {
  private readonly handlersInFlight = new HandlersInFlight();
  private consumerChannel?: { cancel(consumerTag: string): Promise<unknown> };

  async setupChannel(channel: any, callback: Function) {
    this.consumerChannel = channel;
    return super.setupChannel(channel, callback);
  }

  protected runWithProcessingHooks(
    context: BaseRpcContext,
    fn: ProcessingFn,
    endHookContext?: BaseRpcContext,
  ) {
    return super.runWithProcessingHooks(
      context,
      this.handlersInFlight.track(fn),
      endHookContext,
    );
  }

  async close() {
    await this.consumerChannel?.cancel(this.options.consumerTag!);
    await this.handlersInFlight.settled();
    return super.close();
  }
}
