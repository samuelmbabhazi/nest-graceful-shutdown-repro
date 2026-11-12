/**
 * Two instances, A and B, consume one RabbitMQ queue. Every request takes 2 s.
 * A is closed 0.5 s after two requests arrived, and six more are sent while it closes.
 *
 *   npm run two-instances -- none       A is a plain 12.5.0 server
 *   npm run two-instances -- userland   A counts handlers in a global interceptor and waits in beforeApplicationShutdown
 *   npm run two-instances -- proposal   A cancels its consumer, waits for its handlers, then closes
 *
 * Flags:
 *   --signal   close A through the sequence a termination signal runs (hooks first, then the transport)
 *              instead of app.close()
 *   --ack      servers use noAck: false and acknowledge each message once the work is done
 */
import 'reflect-metadata';
import {
  BeforeApplicationShutdown,
  CallHandler,
  Controller,
  ExecutionContext,
  Injectable,
  Module,
  NestInterceptor,
} from '@nestjs/common';
import { APP_INTERCEPTOR, NestFactory } from '@nestjs/core';
import {
  ClientProxyFactory,
  Ctx,
  MessagePattern,
  MicroserviceOptions,
  Payload,
  RmqContext,
  Transport,
} from '@nestjs/microservices';
import { finalize, firstValueFrom, timeout } from 'rxjs';
import { DrainingServerRMQ } from './proposal/draining-servers.js';
import { seconds, sleep } from './timeline.js';

const mode = process.argv[2];
const viaSignal = process.argv.includes('--signal');
const manualAck = process.argv.includes('--ack');
if (!['none', 'userland', 'proposal'].includes(mode)) {
  console.error(
    'usage: npm run two-instances -- <none|userland|proposal> [--signal] [--ack]',
  );
  process.exit(1);
}

const clientOptions = {
  urls: ['amqp://127.0.0.1:15673'],
  queue: `repro_two_${mode}${viaSignal ? '_signal' : ''}${manualAck ? '_ack' : ''}`,
  queueOptions: { durable: true },
};
const serverOptions = { ...clientOptions, noAck: !manualAck };

function createInstance(name: string, waitInUserland = false) {
  const state = { handlersInFlight: 0, taken: [] as number[], completed: 0, failedAcks: 0 };

  @Controller()
  class WorkController {
    @MessagePattern('work')
    async work(@Payload() data: { id: number }, @Ctx() context: RmqContext) {
      state.taken.push(data.id);
      await sleep(2000);
      state.completed++;
      if (manualAck) {
        try {
          context.getChannelRef().ack(context.getMessage());
        } catch {
          state.failedAcks++;
        }
      }
      return name;
    }
  }

  @Injectable()
  class CountingInterceptor implements NestInterceptor {
    intercept(_context: ExecutionContext, next: CallHandler) {
      state.handlersInFlight++;
      return next.handle().pipe(finalize(() => state.handlersInFlight--));
    }
  }

  @Injectable()
  class WaitForHandlers implements BeforeApplicationShutdown {
    async beforeApplicationShutdown() {
      const deadline = Date.now() + 10_000;
      while (state.handlersInFlight > 0 && Date.now() < deadline) {
        await sleep(10);
      }
    }
  }

  @Module({
    controllers: [WorkController],
    providers: waitInUserland
      ? [
          WaitForHandlers,
          { provide: APP_INTERCEPTOR, useClass: CountingInterceptor },
        ]
      : [],
  })
  class AppModule {}

  return { AppModule, state };
}

const a = createInstance('A', mode === 'userland');
const b = createInstance('B');

const appA = await NestFactory.createMicroservice<MicroserviceOptions>(
  a.AppModule,
  mode === 'proposal'
    ? {
        strategy: new DrainingServerRMQ({
          ...serverOptions,
          consumerTag: 'instance-a',
        }),
        logger: false,
      }
    : { transport: Transport.RMQ, options: serverOptions, logger: false },
);
const appB = await NestFactory.createMicroservice<MicroserviceOptions>(
  b.AppModule,
  { transport: Transport.RMQ, options: serverOptions, logger: false },
);
await appA.listen();
await appB.listen();

const client = ClientProxyFactory.create({
  transport: Transport.RMQ,
  options: clientOptions,
});
await client.connect();

const outcomes: { id: number; sentAt: number; outcome: string }[] = [];
const send = (id: number) => {
  const sentAt = seconds();
  return firstValueFrom(
    client.send<string>('work', { id }).pipe(timeout(12_000)),
  ).then(
    by =>
      outcomes.push({
        id,
        sentAt,
        outcome: `reply from ${by} at ${seconds().toFixed(1)}s`,
      }),
    err =>
      outcomes.push({
        id,
        sentAt,
        outcome:
          err.name === 'TimeoutError'
            ? 'NO REPLY, still pending after 12 s'
            : `error "${err.message ?? err}"`,
      }),
  );
};

const requests = [send(1), send(2)];
await sleep(500);

const closeCalledAt = seconds();
// `shutdown()` is what the signal listener registered by enableShutdownHooks() calls.
// Calling it directly keeps this process, which also hosts instance B, alive.
const closing = (
  viaSignal ? (appA as any).shutdown('SIGTERM') : appA.close()
).then(() => seconds());

for (let id = 3; id <= 8; id++) {
  await sleep(200);
  requests.push(send(id));
}
await Promise.all(requests);
const closeReturnedAt: number = await closing;

console.log(
  `=== ${mode}, ${viaSignal ? 'termination signal sequence' : 'app.close()'}${manualAck ? ', noAck: false' : ''}`,
);
for (const { id, sentAt, outcome } of outcomes.sort((x, y) => x.id - y.id)) {
  console.log(`  request ${id} sent at ${sentAt.toFixed(1)}s -> ${outcome}`);
}
const takenWhileClosing = a.state.taken.filter(id => id >= 3);
console.log(
  `  A closed in ${(closeReturnedAt - closeCalledAt).toFixed(1)}s (called at ${closeCalledAt.toFixed(1)}s, returned at ${closeReturnedAt.toFixed(1)}s)`,
);
console.log(
  `  requests sent after A began closing that A still took: ${takenWhileClosing.length} of 6`,
);
if (manualAck) {
  console.log(
    `  work completed ${a.state.completed} time(s) on A and ${b.state.completed} time(s) on B for 8 requests; acks that failed on A: ${a.state.failedAcks}`,
  );
}

await appB.close().catch(() => undefined);
await Promise.resolve(client.close()).catch(() => undefined);
process.exit(0);
