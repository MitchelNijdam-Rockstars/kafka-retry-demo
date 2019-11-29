package nl.mitchelnijdam.kafkaretrytest.consumer

import nl.mitchelnijdam.kafkaretrytest.service.ExceptionService
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class SpringRetryConsumer(private val exceptionService: ExceptionService = ExceptionService()) {

    private val logger: Logger = LoggerFactory.getLogger(SpringRetryConsumer::class.java)

    @KafkaListener(topics = ["test-retry"], containerFactory = "springRetryKafkaFactory")
    fun listenRetry(record: ConsumerRecord<String, String>) {
        logger.info("received 'test-retry' record! (value: ${record.value()})")

        exceptionService.withRecord(record).iFailButWillRecover()
    }

    @KafkaListener(topics = ["test-retry-e-handler"], containerFactory = "springRetryErrorHandlerKafkaFactory")
    fun listenRetryErrorHander(record: ConsumerRecord<String, String>){
        logger.info("received 'test-retry-e-handler' record! (value: ${record.value()})")
    }
}