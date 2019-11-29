package nl.mitchelnijdam.kafkaretrytest.service

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * This service will throw a "transient" exception in the sense that it will succeed after a certain number of retries.
 * Will use some properties from the kafka record in order to maintain a unique attempt counter.
 *
 * @author Mitchel Nijdam on 29-11-2019
 */
class ExceptionService(
        // number of retries after which the fake service will succeed
        private val recoverAfterAttempt: Int = 5,
        // time the service will take, can be used to validate kafka timeouts
        private val sleepMs: Long = 1_000
) {
    // uses unique record identifier to capture attempts
    private val attemptByRecordId: MutableList<RecordAttempt> = mutableListOf()

    data class RecordAttempt(val id: String, var attempts: Int = 0)

    fun withRecord(record: ConsumerRecord<String, String>): FakeService {
        val recordId = "${record.topic()}-${record.value()}"

        var recordAttempt = attemptByRecordId.find { it.id == recordId }

        if (recordAttempt == null) {
            recordAttempt = RecordAttempt(recordId)
            attemptByRecordId.add(recordAttempt)
        }

        recordAttempt.attempts += 1

        return FakeService(recordAttempt, recoverAfterAttempt, sleepMs)
    }

    class FakeService(private val recordAttempt: RecordAttempt, private val recoverAfterAttempt: Int, private val sleepMs: Long) {

        private val logger: Logger = LoggerFactory.getLogger(FakeService::class.java)

        fun iFailButWillRecover() {
            logger.debug("Will do some important things for record ${recordAttempt.id} (${sleepMs / 1000} sec), attempt ${recordAttempt.attempts} of $recoverAfterAttempt")

            Thread.sleep(sleepMs)

            if (recordAttempt.attempts < recoverAfterAttempt) {
                logger.debug("Important things failed :(")

                throw RuntimeException("OUCH I HURT MYSELF! recordId: ${recordAttempt.id}, attempt no ${recordAttempt.attempts}")
            }

            logger.debug("Important things succeeded! resetting attempt counter.")
            recordAttempt.attempts = 0
        }
    }
}