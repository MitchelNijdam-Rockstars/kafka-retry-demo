package nl.mitchelnijdam.kafkaretrytest.configuration

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.KafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory

@Configuration
@EnableKafka
class KafkaBatchConsumerConfiguration {

    @Bean
    fun kafkaBatchFactory(kafkaConsumerFactory: ConsumerFactory<Any, Any>): KafkaListenerContainerFactory<*> {

        val factory = ConcurrentKafkaListenerContainerFactory<Int, String>()
        factory.consumerFactory = kafkaConsumerFactory
        factory.isBatchListener = true

        return factory
    }
}