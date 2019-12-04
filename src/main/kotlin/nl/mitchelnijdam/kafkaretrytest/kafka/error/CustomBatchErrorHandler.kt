package nl.mitchelnijdam.kafkaretrytest.kafka.error

import nl.mitchelnijdam.kafkaretrytest.exception.TransientException
import nl.mitchelnijdam.kafkaretrytest.kafka.seekToCurrent
import nl.mitchelnijdam.kafkaretrytest.kafka.seekToNext
import org.apache.commons.logging.LogFactory
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ConsumerAwareBatchErrorHandler
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.FailedRecordProcessor
import org.springframework.stereotype.Component

/**
 * This will handle exceptions that are thrown out of a batch kafka listener.
 * Since it does not know where in the batch the exception occurred, it will either retry the whole batch or send
 * the batch to a Dead Letter Queue (DLQ), based on the exception type.
 *
 * Name of the DQL topic will be the name of the original topic + "-dlq"
 *
 * @param kafkaTemplate the template to use for sending records to DLQ
 *
 * @author Mitchel Nijdam
 */
@Component
class CustomBatchErrorHandler(kafkaTemplate: KafkaTemplate<Any, Any>) : ConsumerAwareBatchErrorHandler {

    private val logger = LogFactory.getLog(CustomBatchErrorHandler::class.java)

    private val recoverer = DeadLetterPublishingRecoverer(kafkaTemplate) { r, _ -> TopicPartition(r.topic() + "-dlq", -1) }

    override fun handle(thrownException: Exception, records: ConsumerRecords<*, *>, consumer: Consumer<*, *>) {
        logger.debug("Handling exception ${thrownException.cause?.javaClass} for ${records.count()} records with offsets " +
                records.joinToString { it.offset().toString() })

        val rootCause = thrownException.cause // the original exception is always wrapped by a Spring-Kafka exception

        if (rootCause is TransientException) {
            logger.debug("Transient exception, batch will be retried")
            records.seekToCurrent(consumer)
        } else {
            logger.debug("Non-transient exception, batch will be send to DLQ")
            records.forEach { recoverer.accept(it, thrownException) }
            records.seekToNext(consumer)
        }
    }
}
