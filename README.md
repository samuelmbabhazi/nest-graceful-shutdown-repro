# Handlers in flight lose their reply when a NestJS microservice closes

A reproduction for `@nestjs/microservices` 12.5.0, against real brokers.

When the application closes, the Redis, MQTT, RabbitMQ and TCP servers close their connection at once. A request handler that is still running finishes after `onApplicationShutdown`, and its reply is never delivered. NATS behaves the same unless `gracefulShutdown` is set.

## Run it

Node.js 22.12 or later, and Docker.

```bash
npm install
npm run brokers        # Redis, Mosquitto, RabbitMQ and NATS on non-default local ports
npm run single -- redis
```

`npm run brokers:down` stops the brokers.

## 1. One server, one request in flight

`src/single.ts`: a `@MessagePattern()` handler takes 2 s, and `app.close()` is called 0.5 s after the request arrived.

```bash
npm run single -- tcp            # or redis, mqtt, rmq, nats, nats-graceful
```

```text
=== redis
  0.1s  server: handler started
  0.6s  server: app.close() called
  0.6s  server: onApplicationShutdown ran (a database pool would close here)
  0.6s  server: app.close() returned
  2.1s  server: handler finished, resources ALREADY CLOSED
  8.1s  client: NO REPLY, the request is still pending after 8 s
```

| transport | `close()` returns | handler finishes | client |
| --- | --- | --- | --- |
| Redis | 0.6 s | 2.1 s, after `onApplicationShutdown` | no reply, still pending at 8 s |
| MQTT | 0.6 s | 2.1 s, after `onApplicationShutdown` | no reply, still pending at 8 s |
| RabbitMQ | 0.7 s | 2.2 s, after `onApplicationShutdown` | no reply, still pending at 8 s |
| NATS | 0.6 s | 2.1 s, after `onApplicationShutdown` | no reply, still pending at 8 s |
| TCP | 0.5 s | 2.0 s, after `onApplicationShutdown` | `Connection closed` |
| NATS, `gracefulShutdown: true`, `gracePeriod: 5000` | 5.6 s | 2.0 s, before `onApplicationShutdown` | reply at 2.1 s |

With `gracefulShutdown`, NATS delivers the reply, but the grace period is a fixed wait: the handler was done at 2.0 s and `close()` took 5.6 s.

For comparison, `npm run http` runs the same scenario on the Express adapter with default options. The response arrives at 2.2 s and `onApplicationShutdown` runs after it.

## 2. What application code can do today

`src/two-instances.ts`: two instances, A and B, consume one RabbitMQ queue. Every request takes 2 s. A is closed 0.5 s after two requests arrived (one of them on A), and six more are sent while it closes.

```bash
npm run two-instances -- none
npm run two-instances -- userland            # global interceptor that counts handlers + wait in beforeApplicationShutdown
npm run two-instances -- userland --signal   # the sequence a termination signal runs, instead of app.close()
npm run two-instances -- none --signal --ack # servers with noAck: false
```

| A is | closed with | request in flight on A | new requests A still took | A closed in |
| --- | --- | --- | --- | --- |
| a plain server | `app.close()` | no reply | 0 of 6 | 0.0 s |
| counting and waiting in a hook | `app.close()` | no reply | 0 of 6 | 1.5 s |
| counting and waiting in a hook | termination signal | reply from A | 3 of 6 | 3.0 s |

- With `app.close()`, a standalone microservice closes its transport before the first shutdown hook runs, so the wait in the hook comes too late.
- On a termination signal the hooks run first and the reply is saved, but A keeps consuming while it waits, so its shutdown lasts as long as messages keep arriving.
- With `noAck: false` (last command), RabbitMQ redelivers the message to B and the caller gets a reply. The work then runs twice: once on A, whose `ack` fails on the closed channel, and once on B.

## 3. The proposed behaviour

`src/proposal/draining-servers.ts` subclasses `ServerRedis` and `ServerRMQ` so that `close()` stops taking messages, waits for the handlers in flight, then closes. It is a sketch on top of the published package; in the framework the counting would live in `Server`.

```bash
npm run single -- redis --proposal
npm run two-instances -- proposal --signal
```

```text
=== redis (proposal)
  0.1s  server: handler started
  0.6s  server: app.close() called
  2.1s  server: handler finished, resources still open
  2.1s  client: got reply "done"
  2.1s  server: onApplicationShutdown ran (a database pool would close here)
  2.1s  server: app.close() returned
```

With two instances, A answers the request it had in flight, takes none of the six new ones, and closes in 1.6 s.
