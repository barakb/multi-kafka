package com.totango.multikafka

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import java.time.Duration


class Subscribers(group: String) {

    private val consumerProps: MutableMap<String, Any> = HashMap()
    private var subscribers: List<SubscriberContext> = emptyList()

    private val mutex = Mutex()

    init {
        consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        consumerProps[ConsumerConfig.GROUP_ID_CONFIG] = group
        consumerProps[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 1
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = IntegerDeserializer::class.java
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
    }

    suspend fun subscribe(topic: String): Flux<Buffer> {
        val receiverOptions: ReceiverOptions<Int, String> = ReceiverOptions.create<Int, String>(consumerProps)
            .subscription(listOf(topic))
            .addAssignListener { logger.debug("onPartitionsAssigned {}", it) }
            .addRevokeListener { logger.debug("onPartitionsRevoked {}", it) }
        val ctx = SubscriberContext(topic, KafkaReceiver.create(receiverOptions).receive())
        mutex.withLock {
            subscribers = subscribers + ctx
        }
        return ctx.subscriber().bufferTimeout(10, Duration.ofSeconds(5))
            .map { Buffer(topic, it) }
    }

    suspend fun unsubscribe(topic: String) {
        mutex.withLock {
            val (withTopic, withoutTopic) = subscribers.partition {
                it.topic == topic
            }
            subscribers = withoutTopic
            withTopic.forEach { it.close() }
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Subscribers::class.java)
    }

}