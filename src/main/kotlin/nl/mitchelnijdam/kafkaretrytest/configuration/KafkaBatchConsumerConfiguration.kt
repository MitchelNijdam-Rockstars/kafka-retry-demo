package nl.mitchelnijdam.kafkaretrytest.configuration

import nl.mitchelnijdam.kafkaretrytest.exception.TransientException
import nl.mitchelnijdam.kafkaretrytest.kafka.error.SimpleCustomBatchErrorHandler
import nl.mitchelnijdam.kafkaretrytest.kafka.error.ExtensiveCustomBatchErrorHandler
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.KafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler
import org.springframework.util.backoff.ExponentialBackOff
import org.springframework.util.backoff.FixedBackOff

@Configuration
@EnableKafka
class KafkaBatchConsumerConfiguration {

    @Bean
    fun kafkaBatchFactory(kafkaConsumerFactory: ConsumerFactory<Any, Any>): KafkaListenerContainerFactory<*> {

        val factory = ConcurrentKafkaListenerContainerFactory<Int, String>()
        factory.consumerFactory = kafkaConsumerFactory
        factory.isBatchListener = true

        /**
         * From documentation: Since this error handler has no mechanism to "recover" after retries are exhausted,
         * if the BackOffExecution returns STOP, the previous interval will be used for all subsequent delays.
         * The maximum delay must be less than the max.poll.interval.ms consumer property.
         *
         * https://docs.spring.io/spring-kafka/reference/html/#seek-to-current
         */
        val errorHandler = SeekToCurrentBatchErrorHandler()
        errorHandler.setBackOff(FixedBackOff(BACK_OFF_PERIOD, 1L))
        factory.setBatchErrorHandler(errorHandler)

        return factory
    }

    @Bean
    fun customErrorHandlerKafkaBatchFactory(
            kafkaConsumerFactory: ConsumerFactory<Any, Any>,
            simpleCustomBatchErrorHandler: SimpleCustomBatchErrorHandler
    ): KafkaListenerContainerFactory<*> {

        val factory = ConcurrentKafkaListenerContainerFactory<Int, String>()
        factory.consumerFactory = kafkaConsumerFactory
        factory.isBatchListener = true
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL_IMMEDIATE

        factory.setBatchErrorHandler(simpleCustomBatchErrorHandler)

        return factory
    }

    @Bean
    fun extensiveCustomErrorHandlerKafkaBatchFactory(
            kafkaConsumerFactory: ConsumerFactory<Any, Any>,
            kafkaTemplate: KafkaTemplate<Any, Any>
    ): KafkaListenerContainerFactory<*> {

        val factory = ConcurrentKafkaListenerContainerFactory<Int, String>()

        factory.consumerFactory = kafkaConsumerFactory
        factory.isBatchListener = true

        val backOff =  FixedBackOff(BACK_OFF_PERIOD, MAX_ATTEMPTS.toLong())
        //val backOff =  ExponentialBackOff()
        //backOff.initialInterval = 1_000L
        //backOff.maxInterval = 30_000L // this should not be bigger than max.poll.interval.ms
        //backOff.maxElapsedTime = 30_000L // total elapsed time, only way to stop the backoff

        val errorHandler = ExtensiveCustomBatchErrorHandler(kafkaTemplate, backOff)
        errorHandler.addRetryableException(TransientException::class.java)
        factory.setBatchErrorHandler(errorHandler)

        return factory
    }
}