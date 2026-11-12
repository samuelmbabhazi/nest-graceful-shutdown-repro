/**
 * A request handler takes 2 s. The server application is closed 0.5 s after the request arrived.
 *
 *   npm run single -- redis              tcp | redis | mqtt | rmq | nats | nats-graceful
 *   npm run single -- redis --proposal   the same with the proposed behaviour (redis only)
 */
import 'reflect-metadata';
import {
  Controller,
  Inject,
  Injectable,
  Module,
  OnApplicationShutdown,
} from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import {
  ClientOptions,
  ClientProxyFactory,
  MessagePattern,
  MicroserviceOptions,
} from '@nestjs/microservices';
import { firstValueFrom, timeout } from 'rxjs';
import { DrainingServerRedis } from './proposal/draining-servers.js';
import { mark, print, seconds, sleep } from './timeline.js';
import { TransportName, transports } from './transports.js';

const name = process.argv[2] as TransportName;
const useProposal = process.argv.includes('--proposal');
const config = transports[name];
if (!config || (useProposal && name !== 'redis')) {
  console.error(
    'usage: npm run single -- <tcp|redis|mqtt|rmq|nats|nats-graceful> [--proposal (redis only)]',
  );
  process.exit(1);
}

@Injectable()
class Resources implements OnApplicationShutdown {
  closed = false;

  onApplicationShutdown() {
    this.closed = true;
    mark('server: onApplicationShutdown ran (a database pool would close here)');
  }
}

@Controller()
class SlowController {
  constructor(@Inject(Resources) private readonly resources: Resources) {}

  @MessagePattern('slow')
  async slow() {
    mark('server: handler started');
    await sleep(2000);
    mark(
      `server: handler finished, resources ${this.resources.closed ? 'ALREADY CLOSED' : 'still open'}`,
    );
    return 'done';
  }
}

@Module({ controllers: [SlowController], providers: [Resources] })
class AppModule {}

const app = await NestFactory.createMicroservice<MicroserviceOptions>(
  AppModule,
  useProposal
    ? {
        strategy: new DrainingServerRedis(transports.redis.options),
        logger: false,
      }
    : { ...config, logger: false },
);
await app.listen();

const client = ClientProxyFactory.create(config as ClientOptions);
await client.connect();

const reply = firstValueFrom(client.send('slow', {}).pipe(timeout(8000))).then(
  value => mark(`client: got reply "${value}"`),
  err =>
    mark(
      err.name === 'TimeoutError'
        ? 'client: NO REPLY, the request is still pending after 8 s'
        : `client: error "${err.message ?? err}"`,
    ),
);

await sleep(500);
mark('server: app.close() called');
await app.close();
mark('server: app.close() returned');

await reply;
// Leave the handler time to finish, so that its last line is in the output.
await sleep(Math.max(0, 2800 - seconds() * 1000));

print(`${name}${useProposal ? ' (proposal)' : ''}`);
await Promise.resolve(client.close()).catch(() => undefined);
process.exit(0);
