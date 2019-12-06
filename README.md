# Examples of retry mechanisms for spring-kafka consumer

For kafka consumers there are several approaches on handling errors. One is sending each erroring record to a DLQ. In that case a person needs to look at the errors and act on it (for example replay the message or ignore it). However for transient errors, that is: those that are temporary and will resolve itself, this manual process does not make a lot of sense and a waste of time for that person.

This is where this project comes in: it provides some examples on how to handle transient exceptions for different types of kafka consumers and error handlers. Use this project as example on different retry mechanisms for kafka consumers using [spring-kafka](https://github.com/spring-projects/spring-kafka) library.


### Setup
Make sure you have a local kafka instance running. Configure your broker ip in `application.yml` in `bootstrap-servers: <local_kafka_ip>:9092`.

Project uses Java 11.

### Local
Produce a kafka message with the tool `kafkacat` to any topic used by a `@kafkaListener`.

```shell script
kafkacat -b <local_kafka_ip>:9092 -t test-retry -P <<< 'CallMeKafkaRecord'
```

### Test
Each retry mechanism has its own message listener, all consuming from a different topic, so that you can compare them by sending messages to different topics while the application is running. 

In order to use the different retry mechanisms, first check which one you would like to use (see `KafkaConsumerConfiguration.kt` and `KafkaBatchConsumerConfiguration.kt`). Then check which listener is using the particular ContainerFactory and look at which topic it is consuming.

### Batch error handling
Since error handling of batch listeners is quite a bit different from single message listeners, the configuration and consumers are seperated. I've added two custom batch error handlers, one simple and one more extensive.

The extensive batch error handler uses a `BinaryExceptionClassifier` in order to identify transient exceptions. You have to provide these in the `KafkaBatchConsumerConfiguration.kt`. It also has a custom implementation of the `BackOff` so you can use different stategies (like `FixedBackOff` or `ExponentialBackOff`). If either the exception is not a transient exception _or_ the backoff is expired for the current batch, it will send all records to the DLQ.

NOTE: only use this approach when you are sure you can retry the whole batch.
