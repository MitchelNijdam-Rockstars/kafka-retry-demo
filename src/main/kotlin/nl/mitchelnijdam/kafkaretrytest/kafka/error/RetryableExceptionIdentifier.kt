package nl.mitchelnijdam.kafkaretrytest.kafka.error

import org.springframework.classify.BinaryExceptionClassifier

/**
 * Saves a list of [Exception] classes that are considered 'retryable' from kafka's point of view.
 *
 * Add classes to the list with [addRetryableException].
 * Test if a Throwable class is a retryable exception by using [shouldRetry]. If one of the causes is a retryable
 * Exception, it will also return true.
 *
 * @author Mitchel Nijdam
 */
class RetryableExceptionIdentifier {

    private val retryableExceptionClassifier: BinaryExceptionClassifier = ExtendedBinaryExceptionClassifier(emptyMap(), false)

    fun addRetryableException(exceptionType: Class<out Exception>) {
        (retryableExceptionClassifier as ExtendedBinaryExceptionClassifier).classified[exceptionType] = true
    }

    fun shouldRetry(rootCause: Throwable?): Boolean = retryableExceptionClassifier.classify(rootCause)

    /**
     *  Extended to provide visibility to the current classified exceptions.
     */
    private class ExtendedBinaryExceptionClassifier internal constructor(
            typeMap: Map<Class<out Throwable>, Boolean>,
            defaultValue: Boolean
    ) : BinaryExceptionClassifier(typeMap, defaultValue) {

        public override fun getClassified(): MutableMap<Class<out Throwable>, Boolean> {
            return super.getClassified()
        }

        init {
            setTraverseCauses(true)
        }
    }
}