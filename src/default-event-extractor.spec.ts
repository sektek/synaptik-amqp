import { expect } from 'chai';

import { ConsumeMessage } from 'amqplib';
import { Event } from '@sektek/synaptik';

import { defaultEventExtractor } from './default-event-extractor.js';

const toMessage = (content: string): ConsumeMessage =>
  ({ content: Buffer.from(content) }) as ConsumeMessage;

describe('defaultEventExtractor', function () {
  it('parses the JSON-encoded message content into an event', function () {
    const event: Event = { id: 'event-1', type: 'TestEvent', data: {} };

    expect(defaultEventExtractor(toMessage(JSON.stringify(event)))).to.eql(
      event,
    );
  });

  it('throws when the message content is not valid JSON', function () {
    expect(() => defaultEventExtractor(toMessage('not json'))).to.throw(
      SyntaxError,
    );
  });
});
