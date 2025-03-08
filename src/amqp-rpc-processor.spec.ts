import { randomUUID } from 'crypto';

import { expect, use } from 'chai';
import sinon, { fake, match } from 'sinon';
import chaiAsPromised from 'chai-as-promised';
import sinonChai from 'sinon-chai';

import { Channel, Connection, ConsumeMessage, Replies, connect } from 'amqplib';
import { Event, EventBuilder } from '@sektek/synaptik';

import { AmqpChannel } from './amqp-channel.js';
import { AmqpRpcProcessor } from './amqp-rpc-processor.js';
import { DefaultChannelProvider } from './default-channel-provider.js';

use(chaiAsPromised);
use(sinonChai);

const AMQP_HOST = process.env.AMQP_HOST || 'localhost';
const WAIT_TIME = 500;

type MessageHandler = (msg: ConsumeMessage) => Promise<void> | void;

const createConsumer = async (
  channel: Channel,
  queueName: string,
  onMessage: MessageHandler,
) => {
  return await channel.consume(
    queueName,
    async (message: ConsumeMessage | null) => {
      if (message) {
        await onMessage(message);
        await channel.ack(message);
      }
    },
  );
};

const respond = async (
  channel: Channel,
  message: ConsumeMessage,
  event: Event,
  correlationId?: string,
) => {
  channel.publish(
    '',
    message.properties.replyTo,
    Buffer.from(JSON.stringify(event)),
    {
      correlationId: correlationId ?? message.properties.correlationId,
    },
  );
};

describe('AmqpRpcProcessor', function () {
  let queueName: string;
  let connection: Connection;
  let channelProvider: DefaultChannelProvider;
  let outboundChannel: AmqpChannel;
  let consumer: Replies.Consume | null = null;

  beforeEach(async function () {
    queueName = `test-queue-${randomUUID()}`;
    connection = await connect({ hostname: AMQP_HOST });
    channelProvider = new DefaultChannelProvider({ connection });
    const channel = await channelProvider.get();
    await channel.assertQueue(queueName);
    outboundChannel = new AmqpChannel({ channelProvider, queueName });
  });

  afterEach(async function () {
    const channel = await channelProvider.get();
    await channel.deleteQueue(queueName);
    if (consumer) {
      await channel.cancel(consumer.consumerTag);
      consumer = null;
    }
    await connection.close();
    sinon.reset();
  });

  it('sends a request with a correlation id', async function () {
    const event = await EventBuilder.create();
    const processor = new AmqpRpcProcessor({
      channelProvider,
      outboundChannel,
    });

    const channel = await channelProvider.get();
    consumer = await createConsumer(channel, queueName, async message => {
      expect(message.properties?.correlationId).to.exist;
      respond(channel, message, event);
    });

    await processor.process(event);
  });

  it('sends a request with a replyTo queue', async function () {
    const event = await EventBuilder.create();
    const processor = new AmqpRpcProcessor({
      channelProvider,
      outboundChannel,
    });

    const channel = await channelProvider.get();
    consumer = await createConsumer(channel, queueName, async message => {
      expect(message.properties?.replyTo).to.exist;
      respond(channel, message, event);
    });

    const response = await processor.process(event);
    expect(response).to.deep.equal(event);
  });

  it('resolves the promise with the response', async function () {
    const event = await EventBuilder.create();
    const processor = new AmqpRpcProcessor({
      channelProvider,
      outboundChannel,
    });

    const channel = await channelProvider.get();
    consumer = await createConsumer(channel, queueName, async message => {
      respond(channel, message, event);
    });

    const response = await processor.process(event);
    expect(response).to.deep.equal(event);
  });

  it('rejects the promise on timeout', async function () {
    const event = await EventBuilder.create();
    const processor = new AmqpRpcProcessor({
      channelProvider,
      outboundChannel,
      timeout: WAIT_TIME,
    });

    const channel = await channelProvider.get();
    consumer = await createConsumer(channel, queueName, async () => {});

    await expect(processor.process(event)).to.be.rejected;
  });

  describe('event emitter', function () {
    it('should emit a message:received event', async function () {
      const event = await EventBuilder.create();
      const processor = new AmqpRpcProcessor({
        channelProvider,
        outboundChannel,
      });

      const listener = fake();
      processor.on('message:received', listener);

      const channel = await channelProvider.get();
      consumer = await createConsumer(channel, queueName, async message => {
        respond(channel, message, event);
      });

      await processor.process(event);
      expect(listener).to.have.been.called;
    });

    it('should emit an event:received event', async function () {
      const event = await EventBuilder.create();
      const processor = new AmqpRpcProcessor({
        channelProvider,
        outboundChannel,
      });

      const listener = fake();
      processor.on('event:received', listener);

      const channel = await channelProvider.get();
      consumer = await createConsumer(channel, queueName, async message => {
        respond(channel, message, event);
      });

      await processor.process(event);
      expect(listener).to.have.been.calledWith(event);
    });

    it('should emit an event:processed event', async function () {
      const event = await EventBuilder.create();
      const result = await EventBuilder.create();
      const processor = new AmqpRpcProcessor({
        channelProvider,
        outboundChannel,
      });

      const listener = fake();
      processor.on('event:processed', listener);

      const channel = await channelProvider.get();
      consumer = await createConsumer(channel, queueName, async message => {
        respond(channel, message, result);
      });

      await processor.process(event);
      expect(listener).to.have.been.calledWith(event, result);
    });

    it('should emit a message:processed event', async function () {
      const event = await EventBuilder.create();
      const result = await EventBuilder.create();
      const processor = new AmqpRpcProcessor({
        channelProvider,
        outboundChannel,
        extractor: async message => JSON.parse(message.content.toString()),
      });

      const listener = fake();
      processor.on('message:processed', listener);

      const channel = await channelProvider.get();
      consumer = await createConsumer(channel, queueName, async message => {
        respond(channel, message, result);
      });

      await processor.process(event);
      expect(listener).to.have.been.calledWith(match.any, event, result);
    });

    it('should emit an event:error event', async function () {
      const event = await EventBuilder.create();
      const error = new Error('Test error');
      const processor = new AmqpRpcProcessor({
        channelProvider,
        outboundChannel,
        timeout: WAIT_TIME,
        extractor: async () => {
          throw error;
        },
      });

      const listener = fake();
      processor.on('event:error', listener);

      const channel = await channelProvider.get();
      consumer = await createConsumer(channel, queueName, async message => {
        respond(channel, message, event);
      });

      expect(processor.process(event)).to.be.rejectedWith(error);
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));
      expect(listener).to.have.been.calledWith(event, error);
    });

    it('should emit a message:error event', async function () {
      const event = await EventBuilder.create();
      const error = new Error('Test error');
      const processor = new AmqpRpcProcessor({
        channelProvider,
        outboundChannel,
        extractor: async () => {
          throw error;
        },
      });

      const listener = fake();
      processor.on('message:error', listener);

      const channel = await channelProvider.get();
      consumer = await createConsumer(channel, queueName, async message => {
        respond(channel, message, event);
      });

      expect(processor.process(event)).to.be.rejectedWith(error);
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));
      expect(listener).to.have.been.calledWith(match.any, error);
    });

    it('should emit a message:error event when the correlation id does not match', async function () {
      const event = await EventBuilder.create();
      const processor = new AmqpRpcProcessor({
        channelProvider,
        outboundChannel,
      });

      const listener = fake();
      processor.on('message:error', listener);

      const channel = await channelProvider.get();
      consumer = await createConsumer(channel, queueName, async message => {
        respond(channel, message, event, 'wrong-correlation-id');
      });

      expect(processor.process(event)).to.be.rejectedWith(
        /Expected correlationId/,
      );
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));
      expect(listener).to.have.been.calledWith(
        match.typeOf('object'),
        match
          .instanceOf(Error)
          .and(match.has('message', match(/Expected correlationId/))),
      );
    });

    it('should emit an event:error event when the correlation id does not match', async function () {
      const event = await EventBuilder.create();
      const processor = new AmqpRpcProcessor({
        channelProvider,
        outboundChannel,
      });

      const listener = fake();
      processor.on('event:error', listener);

      const channel = await channelProvider.get();
      consumer = await createConsumer(channel, queueName, async message => {
        respond(channel, message, event, 'wrong-correlation-id');
      });

      expect(processor.process(event)).to.be.rejectedWith(
        /Expected correlationId/,
      );
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));
      expect(listener).to.have.been.calledWith(
        match.typeOf('object'),
        match
          .instanceOf(Error)
          .and(match.has('message', match(/Expected correlationId/))),
      );
    });
  });
});
