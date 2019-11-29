# Examples of retry mechanisms for spring-kafka consumer

For kafka consumers there are several approaches on handling errors. One is sending each erroring record to a DLQ. In that case a person needs to look at the errors and act on it (for example replay the message or ignore it). However for transient errors, that is: those that are temporary and will resolve itself, this manual process does not make a lot of sense and a waste of time for that person.

This is where this project comes in: it provides some examples on how to handle transient exceptions for different types of kafka consumers and error handlers. Use this project as example on different retry mechanisms for kafka consumers using [spring-kafka](https://github.com/spring-projects/spring-kafka) library.

### Setup
Make sure you have a local kafka instance running. Configure your broker ip in `application.yml` in `bootstrap-servers: <local_kafka_ip>:9092`. Also make sure all topics exist or the 

Run with Java 11.

### Local
Produce kafka message(s) with the tool `kafkacat`.

```shell script
kafkacat -b <local_kafka_ip>:9092 -t test-retry -P <<< 'CallMeKafkaRecord'
```