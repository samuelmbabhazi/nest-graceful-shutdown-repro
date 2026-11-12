import { MicroserviceOptions, Transport } from '@nestjs/microservices';

/** Ports match docker-compose.yml. */
export const transports = {
  tcp: {
    transport: Transport.TCP,
    options: { host: '127.0.0.1', port: 18877 },
  },
  redis: {
    transport: Transport.REDIS,
    options: { host: '127.0.0.1', port: 16379 },
  },
  mqtt: {
    transport: Transport.MQTT,
    options: { url: 'mqtt://127.0.0.1:11883' },
  },
  rmq: {
    transport: Transport.RMQ,
    options: {
      urls: ['amqp://127.0.0.1:15673'],
      queue: 'repro_single',
      queueOptions: { durable: true },
    },
  },
  nats: {
    transport: Transport.NATS,
    options: { servers: ['nats://127.0.0.1:14222'] },
  },
  'nats-graceful': {
    transport: Transport.NATS,
    options: {
      servers: ['nats://127.0.0.1:14222'],
      gracefulShutdown: true,
      gracePeriod: 5000,
    },
  },
} satisfies Record<string, MicroserviceOptions>;

export type TransportName = keyof typeof transports;
