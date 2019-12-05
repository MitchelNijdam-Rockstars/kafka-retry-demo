package nl.mitchelnijdam.kafkaretrytest.kafka

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.TopicPartition

/**
 * Custom seek function which will seek to the next (max + 1) offset for each [TopicPartition] in the [ConsumerRecords].
 *
 * @author Mitchel Nijdam
 */
fun <K, V> ConsumerRecords<K, V>.seekToNext(consumer: Consumer<*, *>) {
    this.toList()
            .groupBy { TopicPartition(it.topic(), it.partition()) }
            .mapValues { (_, value) ->
                value.maxBy { it.offset() }!!.offset()
            }.forEach { (topicPartition, maxOffset) ->
                consumer.seek(topicPartition, maxOffset + 1)
                consumer.commitSync()
            }
}

/**
 * Custom seek function which will seek to the first/smallest offset for each [TopicPartition] in all [ConsumerRecords].
 */
fun <K, V> ConsumerRecords<K, V>.seekToCurrent(consumer: Consumer<*, *>) {
    this.toList()
            .groupBy { TopicPartition(it.topic(), it.partition()) }
            .mapValues { (_, value) ->
                value.minBy { it.offset() }!!.offset()
            }.forEach { (topicPartition, minOffset) ->
                consumer.seek(topicPartition, minOffset)
                consumer.commitSync()
            }
}