package nl.mitchelnijdam.kafkaretrytest.configuration

import nl.mitchelnijdam.kafkaretrytest.kafka.error.CustomBatchErrorHandler
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.KafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler
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
            customBatchErrorHandler: CustomBatchErrorHandler
    ): KafkaListenerContainerFactory<*> {

        val factory = ConcurrentKafkaListenerContainerFactory<Int, String>()
        factory.consumerFactory = kafkaConsumerFactory
        factory.isBatchListener = true

        factory.setBatchErrorHandler(customBatchErrorHandler)

        return factory
    }
}