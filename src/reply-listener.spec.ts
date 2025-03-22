import { randomUUID } from 'crypto';

import { expect, use } from 'chai';
import sinon, { spy } from 'sinon';
import chaiAsPromised from 'chai-as-promised';
import sinonChai from 'sinon-chai';

import { Connection, connect } from 'amqplib';
import { Event, EventBuilder } from '@sektek/synaptik';

import { AmqpChannel } from './amqp-channel.js';
import { AmqpGateway } from './amqp-gateway.js';
import { DefaultChannelProvider } from './default-channel-provider.js';
import { ReplyListener } from './reply-listener.js';

use(sinonChai);
use(chaiAsPromised);

const AMQP_HOST = process.env.AMQP_HOST || 'localhost';
const WAIT_TIME = 500;

describe('ReplyListener', function () {
  let connection: Connection;
  let channelProvider: DefaultChannelProvider;
  let event: Event, replyEvent: Event;
  let eventChannel: AmqpChannel;
  let gateway: AmqpGateway<Event, Event>;
  let queueName: string = '';
  let replyTo: string = '';

  beforeEach(async function () {
    connection = await connect({ hostname: AMQP_HOST });
    event = await EventBuilder.create();
    replyEvent = await EventBuilder.create();

    queueName = `test-queue-${randomUUID()}`;
    channelProvider = new DefaultChannelProvider({ connection });
    const channel = await channelProvider.get();
    await channel.assertQueue(queueName);
    const replyQueue = await channel.assertQueue('', { exclusive: true });
    replyTo = replyQueue.queue;

    eventChannel = new AmqpChannel({ queueName, channelProvider });
    gateway = new AmqpGateway({
      queueName,
      channelProvider,
      handler: () => {
        return replyEvent;
      },
    });
  });

  afterEach(async function () {
    gateway.stop();
    const channel = await channelProvider.get();
    await channel.deleteQueue(queueName);
    await channel.deleteQueue(replyTo);
    await connection.close();
    sinon.reset();
  });

  it('should send a reply message when message is processed', async function () {
    const outboundChannel = new AmqpChannel({ channelProvider });
    const sendSpy = spy(outboundChannel, 'send');
    new ReplyListener({ outboundChannel, gateway });

    await eventChannel.send(event, {
      correlationId: event.id,
      replyTo,
    });
    await gateway.start();
    await new Promise(resolve => setTimeout(resolve, WAIT_TIME));

    expect(sendSpy).to.have.been.calledOnce;
    expect(sendSpy).to.have.been.calledWith(
      replyEvent,
      sinon.match({
        correlationId: event.id,
        routingKey: replyTo,
      }),
    );
  });
});
