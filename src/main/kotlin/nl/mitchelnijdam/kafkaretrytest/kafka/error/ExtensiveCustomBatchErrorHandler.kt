package nl.mitchelnijdam.kafkaretrytest.kafka.error

import nl.mitchelnijdam.kafkaretrytest.kafka.seekToCurrent
import nl.mitchelnijdam.kafkaretrytest.kafka.seekToNext
import org.apache.commons.logging.LogFactory
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition
import org.springframework.classify.BinaryExceptionClassifier
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ConsumerAwareBatchErrorHandler
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer
import org.springframework.kafka.listener.FailedRecordProcessor
import org.springframework.util.backoff.BackOff

/**
 * This will handle exceptions that are thrown out of a batch kafka listener.
 * Since it does not know where in the batch the exception occurred, it will either retry the whole batch or send
 * the batch to a Dead Letter Queue (DLQ), based on the exception type.
 *
 * Name of the DQL topic will be the name of the original topic + "-dlq"
 *
 * Used [FailedRecordProcessor] as inspiration.
 *
 * @param kafkaTemplate the template to use for sending records to DLQ
 * @param backOff the waiting strategy to use between retryable errors
 *
 * @author Mitchel Nijdam
 */
class ExtensiveCustomBatchErrorHandler(private val kafkaTemplate: KafkaTemplate<Any, Any>, private val backOff: BackOff)
    : ConsumerAwareBatchErrorHandler {

    private val logger = LogFactory.getLog(ExtensiveCustomBatchErrorHandler::class.java)

    private val recoverer = DeadLetterPublishingRecoverer(kafkaTemplate) { r, _ -> TopicPartition(r.topic() + "-dlq", -1) }

    // should contain classes that are considered retryable
    private val retryableExceptionClassifier: BinaryExceptionClassifier = ExtendedBinaryExceptionClassifier(emptyMap(), false)
    private val retryableRecordsProcessor = RetryableRecordBatchProcessor(backOff)

    override fun handle(thrownException: Exception, records: ConsumerRecords<*, *>, consumer: Consumer<*, *>) {
        logger.debug("Handling exception ${thrownException.cause?.javaClass} for ${records.count()} records with offsets " +
                records.joinToString { it.offset().toString() })

        logger.debug("CURRENT ErrorHandler THREAD ID: ${Thread.currentThread().id}")

        val rootCause = thrownException.cause

        if (retryableExceptionClassifier.classify(rootCause)) {
            logger.debug("Exception is retryable!")
            retryableRecordsProcessor.seekToCurrent(records, consumer) // this uses BackOff
        } else {
            logger.debug("Exception is not retryable, sending batch to DLQ!")
            records.forEach { recoverer.accept(it, thrownException) }
            records.seekToNext(consumer)
        }
    }

    fun addRetryableException(exceptionType: Class<out Exception>) {
        (retryableExceptionClassifier as ExtendedBinaryExceptionClassifier).classified[exceptionType] = true
    }

    /**
     *  Extended to provide visibility to the current classified exceptions.
     */
    private class ExtendedBinaryExceptionClassifier internal constructor(typeMap: Map<Class<out Throwable?>?, Boolean?>?, defaultValue: Boolean)
        : BinaryExceptionClassifier(typeMap, defaultValue) {

        public override fun getClassified(): MutableMap<Class<out Throwable?>, Boolean> {
            return super.getClassified()
        }

        init {
            setTraverseCauses(true)
        }
    }
}

