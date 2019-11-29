package nl.mitchelnijdam.kafkaretrytest.consumer

import nl.mitchelnijdam.kafkaretrytest.configuration.KafkaBatchConsumerConfiguration
import nl.mitchelnijdam.kafkaretrytest.service.ExceptionService
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

/**
 * This consumer is configured as a batchListener (see [KafkaBatchConsumerConfiguration.kafkaBatchFactory]).
 * Since spring-kafka doesn't provide a retry adapter for batch listeners, this has a somewhat coupled retry implementation.
 *
 * @author Mitchel Nijdam on 28-11-2019
 */
@Component
class SpringRetryBatchConsumer {

    private val logger: Logger = LoggerFactory.getLogger(SpringRetryBatchConsumer::class.java)

    @KafkaListener(topics = ["test-retry-batch"], containerFactory = "kafkaBatchFactory")
    fun listen(records: List<ConsumerRecord<String, String>>, consumer: Consumer<*, *>) {
        logger.info("received ${records.size} 'test-retry-batch' record(s)!")
    }
}