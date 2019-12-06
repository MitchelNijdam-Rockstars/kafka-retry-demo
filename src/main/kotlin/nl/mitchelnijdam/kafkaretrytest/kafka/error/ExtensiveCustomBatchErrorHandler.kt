package nl.mitchelnijdam.kafkaretrytest.kafka.error

import nl.mitchelnijdam.kafkaretrytest.kafka.seekToNext
import org.apache.commons.logging.LogFactory
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ConsumerAwareBatchErrorHandler
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.FailedRecordProcessor
import org.springframework.util.backoff.BackOff

/**
 * This will handle exceptions that are thrown out of a batch kafka listener.
 * Since it does not know where in the batch the exception occurred, it will either retry the whole batch or send
 * the batch to a Dead Letter Queue (DLQ), based on the exception type and backoff policy configured.
 *
 * The DQL topic will be the original topic + "-dlq"
 *
 * Used [FailedRecordProcessor] as inspiration.
 *
 * @param kafkaTemplate the template to use for sending records to DLQ
 * @param backOff the waiting strategy to use between retryable errors
 *
 * @author Mitchel Nijdam
 */
class ExtensiveCustomBatchErrorHandler(private val kafkaTemplate: KafkaTemplate<Any, Any>, backOff: BackOff)
    : ConsumerAwareBatchErrorHandler {

    private val logger = LogFactory.getLog(ExtensiveCustomBatchErrorHandler::class.java)

    private val recoverer = DeadLetterPublishingRecoverer(kafkaTemplate) { r, _ -> TopicPartition(r.topic() + "-dlq", -1) }

    // should contain classes that are considered retryable
    private val retryableExceptionIdentifier = RetryableExceptionIdentifier()
    private val retryableRecordsProcessor = RetryableRecordBatchProcessor(backOff, recoverer)

    override fun handle(thrownException: Exception, records: ConsumerRecords<*, *>, consumer: Consumer<*, *>) {
        logger.warn("Handling exception ${thrownException.cause?.javaClass} for ${records.count()} records with offsets " +
                records.joinToString { it.offset().toString() })

        val rootCause = thrownException.cause // the original exception is always wrapped by a Spring-Kafka exception

        if (retryableExceptionIdentifier.shouldRetry(rootCause)) {
            logger.debug("Exception ${rootCause?.javaClass?.simpleName} is retryable!")
            retryableRecordsProcessor.seekToCurrentOrRecover(records, consumer, thrownException) // this uses BackOff
        } else {
            logger.debug("Exception ${rootCause?.javaClass?.simpleName} is not retryable, sending batch to DLQ!")
            records.forEach { recoverer.accept(it, thrownException) }
            records.seekToNext(consumer)
        }
    }


    // makes sure the offset is committed to kafka after the error handler finished without exceptions
    override fun isAckAfterHandle(): Boolean {
        return true
    }

    fun addRetryableException(exceptionType: Class<out Exception>) {
        retryableExceptionIdentifier.addRetryableException(exceptionType)
    }
}

