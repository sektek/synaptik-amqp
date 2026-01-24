import { randomUUID } from 'crypto';

import { expect, use } from 'chai';
import sinon, { fake, match, spy, stub } from 'sinon';
import sinonChai from 'sinon-chai';

import { Channel, ChannelModel, ConsumeMessage, connect } from 'amqplib';
import { EventBuilder } from '@sektek/synaptik';

import { AmqpChannel } from './amqp-channel.js';
import { AmqpGateway } from './amqp-gateway.js';
import { DefaultChannelProvider } from './default-channel-provider.js';

use(sinonChai);

const AMQP_HOST = process.env.AMQP_HOST || 'localhost';
const WAIT_TIME = 500;

describe('AmqpGateway', function () {
  let queueName: string;
  let connection: ChannelModel, channel: Channel;
  let channelProvider: DefaultChannelProvider;
  let eventChannel: AmqpChannel;

  beforeEach(async function () {
    queueName = `test-queue-${randomUUID()}`;
    connection = await connect({ hostname: AMQP_HOST });
    channelProvider = new DefaultChannelProvider({ connection });
    channel = await channelProvider.get();
    await channel.assertQueue(queueName);
    eventChannel = new AmqpChannel({ channelProvider, queueName });
  });

  afterEach(async function () {
    await channel.deleteQueue(queueName);
    await channel.close();
    await connection.close();
    sinon.reset();
  });

  it('should receive events and send them to the handler', async function () {
    const handler = fake();
    const gateway = new AmqpGateway({ channelProvider, handler, queueName });

    const event = await new EventBuilder().create();
    await eventChannel.send(event);
    await gateway.start();
    await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

    expect(handler).to.have.been.calledWith(event);
  });

  it('should acknowledge messages by default', async function () {
    channel = spy(channel);
    channelProvider = new DefaultChannelProvider({ channel });
    const handler = fake();
    const gateway = new AmqpGateway({ channelProvider, handler, queueName });

    const event = await new EventBuilder().create();
    await eventChannel.send(event);
    await gateway.start();
    await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

    expect(channel.ack).to.have.been.called;
  });

  it('should continue processing messages after an error', async function () {
    const handler = stub().onFirstCall().throws(new Error('Handler error'));
    const gateway = new AmqpGateway({
      channelProvider,
      handler,
      queueName,
      requeueOnError: false,
    });

    const firstEvent = await new EventBuilder().create();
    await eventChannel.send(firstEvent);
    await gateway.start();
    await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

    const secondEvent = await new EventBuilder().create();
    await eventChannel.send(secondEvent);
    await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

    expect(handler).to.have.been.calledTwice;
    expect(handler.secondCall).to.have.been.calledWith(secondEvent);
    await gateway.stop();
  });

  describe('event emitter', function () {
    it('should emit a message:received event', async function () {
      const event = await new EventBuilder().create();
      const handler = fake();
      const listener = (message: ConsumeMessage) => {
        expect(JSON.parse(message.content.toString())).to.deep.equal(event);
      };

      const gateway = new AmqpGateway({ channelProvider, handler, queueName });
      gateway.on('message:received', listener);

      await eventChannel.send(event);
      await gateway.start();
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));
    });

    it('should emit an event:received event', async function () {
      const event = await new EventBuilder().create();
      await eventChannel.send(event);

      const handler = fake();
      const listener = fake();
      const gateway = new AmqpGateway({ channelProvider, handler, queueName });
      gateway.on('event:received', listener);

      await gateway.start();
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

      expect(listener).to.have.been.calledWith(event);
    });

    it('should emit an event:processed event', async function () {
      const event = await new EventBuilder().create();
      const result = await new EventBuilder().create();
      await eventChannel.send(event);

      const handler = fake.returns(result);
      const listener = fake();
      const gateway = new AmqpGateway({ channelProvider, handler, queueName });
      gateway.on('event:processed', listener);

      await gateway.start();
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

      expect(listener).to.have.been.calledWith(event, result);
    });

    it('should emit a message:processed event', async function () {
      const event = await new EventBuilder().create();
      const result = await new EventBuilder().create();
      await eventChannel.send(event);

      const handler = fake.returns(result);
      const listener = fake();
      const gateway = new AmqpGateway({ channelProvider, handler, queueName });
      gateway.on('message:processed', listener);

      await gateway.start();
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

      expect(listener).to.have.been.calledWith(
        match.typeOf('object'),
        event,
        result,
      );
    });

    it('should emit a message:acknowledged event', async function () {
      const event = await new EventBuilder().create();
      await eventChannel.send(event);

      const handler = fake();
      const listener = fake();
      const gateway = new AmqpGateway({ channelProvider, handler, queueName });
      gateway.on('message:acknowledged', listener);

      await gateway.start();
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

      expect(listener).to.have.been.calledWith(
        match.typeOf('object'),
        match.typeOf('object'),
        event,
      );
    });

    it('should not emit a message:acknowledged event when noAck is true', async function () {
      const event = await new EventBuilder().create();
      await eventChannel.send(event);

      const handler = fake();
      const listener = fake();
      const gateway = new AmqpGateway({
        channelProvider,
        handler,
        queueName,
        consumeOptions: { noAck: true },
      });
      gateway.on('message:acknowledged', listener);

      await gateway.start();
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

      expect(listener).to.not.have.been.called;
    });

    it('should emit an event:error on error', async function () {
      const event = await new EventBuilder().create();
      const error = new Error('Handler error');
      await eventChannel.send(event);

      const handler = fake.throws(error);
      const listener = fake();
      const gateway = new AmqpGateway({ channelProvider, handler, queueName });
      gateway.on('event:error', listener);

      await gateway.start();
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

      expect(listener).to.have.been.calledWith(error, event);
    });

    it('should emit a message:error on error', async function () {
      const event = await new EventBuilder().create();
      const error = new Error('Handler error');
      await eventChannel.send(event);

      const handler = fake.throws(error);
      const listener = fake();
      const gateway = new AmqpGateway({ channelProvider, handler, queueName });
      gateway.on('message:error', listener);

      await gateway.start();
      await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

      expect(listener).to.have.been.calledWith(error, match.typeOf('object'));
    });
  });
});
