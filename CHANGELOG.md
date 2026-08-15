# Changelog

## 1.1.0 (2026-08-15)

### ⚠ BREAKING CHANGES

* Refactor error handling to be consistent with Utility Belt (#2)

### Features

* add CI/CD workflows ([#5](https://github.com/sektek/synaptik-amqp/issues/5)) ([e6075c9](https://github.com/sektek/synaptik-amqp/commit/e6075c9349dfcc5169905fd0c6fd45cd77a9d3c3))
* Initial library ([#1](https://github.com/sektek/synaptik-amqp/issues/1)) ([e8f449f](https://github.com/sektek/synaptik-amqp/commit/e8f449f2ae9a22f9fcb7c64b44ff901b979d2394))
* Refactor error handling to be consistent with Utility Belt ([#2](https://github.com/sektek/synaptik-amqp/issues/2)) ([a78ec02](https://github.com/sektek/synaptik-amqp/commit/a78ec02809c408d8f6bf3599d2702fe6d5be1f43))

### Bug Fixes

* add missing sinon/chai-as-promised/sinon-chai devDependencies ([#7](https://github.com/sektek/synaptik-amqp/issues/7)) ([067323e](https://github.com/sektek/synaptik-amqp/commit/067323ea4a75f5007d3524e5f6f439f75f4a1b8e))
* bump @sektek/synaptik to 0.4.1 for PromiseChannel error rejection ([#6](https://github.com/sektek/synaptik-amqp/issues/6)) ([1c2d0d6](https://github.com/sektek/synaptik-amqp/commit/1c2d0d6e43f671b9be473b9945bdf4bafe1ca8ad))
* correct typing for channelProvider and publishOptionsProvider ([c8999b5](https://github.com/sektek/synaptik-amqp/commit/c8999b5853e5f75d53b84403dc2ef271401f7252))
* resolve test hang from unclosed connections and leaked ProcessManager listeners ([#4](https://github.com/sektek/synaptik-amqp/issues/4)) ([6ff380d](https://github.com/sektek/synaptik-amqp/commit/6ff380d27453867398a9a7979a3bcbfa9ab10f49)), closes [sektek/utility-belt#47](https://github.com/sektek/utility-belt/issues/47)
* Type issues... ChannelModel instead of Connection ([cde4eb2](https://github.com/sektek/synaptik-amqp/commit/cde4eb25f654c265e3f3fc3c081eecf46a9a218d))
* Updated all calls to getComponent to use options instead of fallback ([2ef7ba0](https://github.com/sektek/synaptik-amqp/commit/2ef7ba0c2372d0317a8be1a6484c0e13ed4213c0))
* updated providers typing ([174ece7](https://github.com/sektek/synaptik-amqp/commit/174ece7fd1c7eee501bfda3411f7e7d494aa0d2e))
