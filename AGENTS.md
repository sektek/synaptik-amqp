# @sektek/synaptik-amqp

RabbitMQ adapter for Synaptik. Keep it a thin translation layer between AMQP messages and the transport-neutral event pipeline.

## Design boundaries

- `AmqpGateway` consumes/acknowledges messages, `AmqpChannel` publishes events, and `AmqpRpcProcessor` owns correlated request/reply behavior.
- Centralize connection/channel lifecycle in the default providers; accept provider components instead of hard-wiring resources.
- Preserve acknowledgement semantics, `noAck`, requeue behavior, correlation IDs, reply queues, backpressure/drain waiting, and lifecycle events when changing transport flow.
- Component types belong in `src/types/`; accept functions or named-method objects through `getComponent`.
- Use `.js` suffixes, document exported declarations/public methods, and update `src/index.ts` plus `src/types/index.ts` for public API changes.

## Testing

- Tests are integration-oriented and require a real RabbitMQ service (`AMQP_HOST`, default `localhost`). Use unique queue names and clean them up.
- Run `npm test`, `npm run lint`, and `npm run build`; use the focused Mocha command for one spec.
- Do not replace transport integration tests with mocked `amqplib`, and do not add dependencies without approval.

