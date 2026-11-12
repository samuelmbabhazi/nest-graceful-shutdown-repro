/**
 * The same scenario over HTTP, for comparison: Express adapter, default options.
 *
 *   npm run http
 */
import 'reflect-metadata';
import {
  Controller,
  Get,
  Inject,
  Injectable,
  Module,
  OnApplicationShutdown,
} from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { mark, print, sleep } from './timeline.js';

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

  @Get('slow')
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

const app = await NestFactory.create(AppModule, { logger: false });
await app.listen(18878, '127.0.0.1');

const response = fetch('http://127.0.0.1:18878/slow').then(
  async res => mark(`client: got response "${await res.text()}"`),
  err => mark(`client: error "${err.cause?.code ?? err.message}"`),
);

await sleep(500);
mark('server: app.close() called');
await app.close();
mark('server: app.close() returned');
await response;

print('http (express, default options)');
process.exit(0);
