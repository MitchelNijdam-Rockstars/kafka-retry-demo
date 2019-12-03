package nl.mitchelnijdam.kafkaretrytest.configuration

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.KafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.SeekToCurrentErrorHandler
import org.springframework.retry.backoff.FixedBackOffPolicy
import org.springframework.retry.policy.SimpleRetryPolicy
import org.springframework.retry.support.RetryTemplate
import org.springframework.util.backoff.FixedBackOff

const val BACK_OFF_PERIOD: Long = 1000L // should not be longer than max.poll.interval.ms
const val MAX_ATTEMPTS: Int = 5

@Configuration
@EnableKafka
class KafkaConsumerConfiguration {


    @Bean
    fun springRetryKafkaFactory(kafkaConsumerFactory: ConsumerFactory<Any, Any>): KafkaListenerContainerFactory<*> {

        val factory = ConcurrentKafkaListenerContainerFactory<Int, String>()
        factory.setRetryTemplate(retryTemplate())
        factory.consumerFactory = kafkaConsumerFactory

        return factory
    }

    @Bean
    fun errorHandlerKafkaFactory(kafkaConsumerFactory: ConsumerFactory<Any, Any>): KafkaListenerContainerFactory<*> {

        val factory = ConcurrentKafkaListenerContainerFactory<Int, String>()
        factory.setErrorHandler(SeekToCurrentErrorHandler(FixedBackOff(BACK_OFF_PERIOD, MAX_ATTEMPTS.toLong())))
        factory.consumerFactory = kafkaConsumerFactory

        return factory
    }

    fun retryTemplate(): RetryTemplate {
        val retryTemplate = RetryTemplate()

        val fixedBackOffPolicy = FixedBackOffPolicy()
        fixedBackOffPolicy.backOffPeriod = BACK_OFF_PERIOD
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy)

        val retryPolicy = SimpleRetryPolicy()
        retryPolicy.maxAttempts = MAX_ATTEMPTS
        retryTemplate.setRetryPolicy(retryPolicy)

        return retryTemplate
    }
}