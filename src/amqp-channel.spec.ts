import { randomUUID } from 'crypto';

import { expect, use } from 'chai';
import chaiAsPromised from 'chai-as-promised';
import { fake } from 'sinon';
import sinonChai from 'sinon-chai';

import { ChannelModel, connect } from 'amqplib';
import { Event, EventBuilder } from '@sektek/synaptik';

import { AmqpChannel } from './amqp-channel.js';
import { DefaultChannelProvider } from './default-channel-provider.js';

use(sinonChai);
use(chaiAsPromised);

const AMQP_HOST = process.env.AMQP_HOST || 'localhost';

describe('AmqpChannel', function () {
  let connection: ChannelModel;
  let channelProvider: DefaultChannelProvider;

  before(async function () {
    connection = await connect({ hostname: AMQP_HOST });
    channelProvider = new DefaultChannelProvider({ connection });
  });

  after(async function () {
    await connection.close();
  });

  afterEach(async function () {
    const channel = await channelProvider.get();
    await channel.close();
  });

  describe('default exchange', function () {
    let queueName: string = '';

    beforeEach(async function () {
      queueName = `test-queue-${randomUUID()}`;
      const channel = await channelProvider.get();
      await channel.assertQueue(queueName);
    });

    afterEach(async function () {
      const channel = await channelProvider.get();
      await channel.deleteQueue(queueName);
    });

    it('should send an event to the queue', async function () {
      const amqpChannel = new AmqpChannel({ channelProvider, queueName });
      const event = await new EventBuilder().create();

      await amqpChannel.send(event);
      const message = await (await channelProvider.get()).get(queueName);
      expect(message).to.not.be.false;
      if (message) {
        expect(JSON.parse(message.content.toString())).to.deep.equal(event);
      }
    });

    it('should send an event to the queue with a routing key', async function () {
      const amqpChannel = new AmqpChannel({
        channelProvider,
        routingKey: queueName,
      });
      const event = await new EventBuilder().create();

      await amqpChannel.send(event);
      const message = await (await channelProvider.get()).get(queueName);
      expect(message).to.not.be.false;
      if (message) {
        expect(JSON.parse(message.content.toString())).to.deep.equal(event);
      }
    });

    it('should send an event to the queue with a routing key provider', async function () {
      const amqpChannel = new AmqpChannel({
        channelProvider,
        routingKeyProvider: () => queueName,
      });
      const event = await new EventBuilder().create();

      await amqpChannel.send(event);
      const message = await (await channelProvider.get()).get(queueName);
      expect(message).to.not.be.false;
      if (message) {
        expect(JSON.parse(message.content.toString())).to.deep.equal(event);
      }
    });
  });

  describe('with exchange', function () {
    let exchange: string, queueName: string;

    beforeEach(async function () {
      exchange = `test-exchange-${randomUUID()}`;
      queueName = `test-queue-${randomUUID()}`;
      const channel = await channelProvider.get();
      await channel.assertExchange(exchange, 'direct');
      await channel.assertQueue(queueName);
      await channel.bindQueue(queueName, exchange, '');
    });

    afterEach(async function () {
      const channel = await channelProvider.get();
      await channel.deleteExchange(exchange);
      await channel.deleteQueue(queueName);
    });

    it('should send an event to the bound queue', async function () {
      const amqpChannel = new AmqpChannel({ channelProvider, exchange });
      const event = await new EventBuilder().create();

      await amqpChannel.send(event);
      const message = await (await channelProvider.get()).get(queueName);
      expect(message).to.not.be.false;
      if (message) {
        expect(JSON.parse(message.content.toString())).to.deep.equal(event);
      }
    });

    it('should send an event to the bound queue with routing key', async function () {
      const queueName = 'second-test-queue';
      const channel = await channelProvider.get();
      await channel.assertQueue(queueName);
      await channel.bindQueue(queueName, exchange, 'test');

      const amqpChannel = new AmqpChannel({
        channelProvider,
        exchange,
        routingKey: 'test',
      });
      const event = await new EventBuilder().create();

      await amqpChannel.send(event);
      const message = await channel.get(queueName);
      expect(message).to.not.be.false;
      if (message) {
        expect(JSON.parse(message.content.toString())).to.deep.equal(event);
      }
      await channel.deleteQueue(queueName);
    });

    it('should send an event to the bound queue with a queue name', async function () {
      const queueName = 'second-test-queue';
      const channel = await channelProvider.get();
      await channel.assertQueue(queueName);
      await channel.bindQueue(queueName, exchange, queueName);

      const amqpChannel = new AmqpChannel({
        channelProvider,
        exchange,
        queueName,
      });
      const event = await new EventBuilder().create();

      await amqpChannel.send(event);
      const message = await channel.get(queueName);
      expect(message).to.not.be.false;
      if (message) {
        expect(JSON.parse(message.content.toString())).to.deep.equal(event);
      }
      await channel.deleteQueue(queueName);
    });

    it('should send an event to the bound queue with a routing key provider', async function () {
      const queueName = 'second-test-queue';
      const channel = await channelProvider.get();
      await channel.assertQueue(queueName);
      await channel.bindQueue(queueName, exchange, 'test');

      const amqpChannel = new AmqpChannel({
        channelProvider,
        exchange,
        routingKeyProvider: () => 'test',
      });
      const event = await new EventBuilder().create();

      await amqpChannel.send(event);
      const message = await channel.get(queueName);
      expect(message).to.not.be.false;
      if (message) {
        expect(JSON.parse(message.content.toString())).to.deep.equal(event);
      }
      await channel.deleteQueue(queueName);
    });
  });

  describe('event emitter', function () {
    const queueName = 'test-queue';

    beforeEach(async function () {
      const channel = await channelProvider.get();
      await channel.assertQueue(queueName);
    });

    afterEach(async function () {
      const channel = await channelProvider.get();
      await channel.deleteQueue(queueName);
    });

    it('should emit an event:received event', async function () {
      const listener = fake();
      const event = await new EventBuilder().create();

      const amqpChannel = new AmqpChannel({ channelProvider, queueName });
      amqpChannel.on('event:received', listener);

      await amqpChannel.send(event);
      expect(listener).to.have.been.calledWith(event);
    });

    it('should emit a message:sent event', async function () {
      const listener = fake();
      const event = await new EventBuilder().create();

      const amqpChannel = new AmqpChannel({ channelProvider, queueName });
      amqpChannel.on('message:sent', listener);

      await amqpChannel.send(event);
      expect(listener).to.have.been.calledWith(
        '',
        queueName,
        { contentType: 'application/json', type: 'Event' },
        JSON.stringify(event),
      );
    });

    it('should emit an event:delivered event', async function () {
      const listener = fake();
      const event = await new EventBuilder().create();

      const amqpChannel = new AmqpChannel({ channelProvider, queueName });
      amqpChannel.on('event:delivered', listener);

      await amqpChannel.send(event);
      expect(listener).to.have.been.calledWith(event);
    });

    it('should emit a event:error event on error', async function () {
      const listener = fake();
      const event = await new EventBuilder().create();

      const amqpChannel = new AmqpChannel({ channelProvider, queueName });
      amqpChannel.on('event:error', listener);

      connection.close();

      await expect(amqpChannel.send(event)).to.eventually.be.rejected;
      expect(listener).to.have.been.calledWith(event);

      // Reestablish the connection for any following tests
      connection = await connect({ hostname: AMQP_HOST });
      channelProvider = new DefaultChannelProvider({ connection });
    });
  });

  describe('with custom serializer', function () {
    let queueName: string = '';

    beforeEach(async function () {
      queueName = `test-queue-${randomUUID()}`;
      const channel = await channelProvider.get();
      await channel.assertQueue(queueName);
    });

    afterEach(async function () {
      const channel = await channelProvider.get();
      await channel.deleteQueue(queueName);
    });

    it('should send the serialized event to the queue', async function () {
      const amqpChannel = new AmqpChannel({
        channelProvider,
        queueName,
        eventSerializer: {
          serialize: (event: Event) => event.data.test as string,
        },
      });
      const event = await new EventBuilder().create({ test: 'test' });

      await amqpChannel.send(event);
      const message = await (await channelProvider.get()).get(queueName);
      expect(message).to.not.be.false;
      if (message) {
        expect(message.content.toString()).to.equal('test');
      }
    });
  });

  describe('with custom publish options provider', function () {
    let queueName: string = '';

    beforeEach(async function () {
      queueName = `test-queue-${randomUUID()}`;
      const channel = await channelProvider.get();
      await channel.assertQueue(queueName);
    });

    afterEach(async function () {
      const channel = await channelProvider.get();
      await channel.deleteQueue(queueName);
    });

    it('should send the event with the provided publish options', async function () {
      const amqpChannel = new AmqpChannel({
        channelProvider,
        queueName,
        publishOptionsProvider: () => ({ replyTo: 'reply-to' }),
      });
      const event = await new EventBuilder().create();

      await amqpChannel.send(event);
      const message = await (await channelProvider.get()).get(queueName);
      expect(message).to.not.be.false;
      if (message) {
        expect(message.properties.replyTo).to.equal('reply-to');
      }
    });
  });
});
