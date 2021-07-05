package com.totango.multikafka

import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.kafka.receiver.ReceiverRecord
import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean

class SubscriberContext(val topic: String, private val subscriber: Flux<ReceiverRecord<Int, String>>) :
    Closeable {

    private val open = AtomicBoolean(true)

    override fun close() {
        logger.info("closing [$this]")
        open.set(false)
    }

    fun subscriber(): Flux<ReceiverRecord<Int, String>> {
        return subscriber.takeWhile { open.get() }
    }

    override fun toString(): String {
        return "SubscriberContext: $topic, open = ${open.get()}"
    }

    companion object{
        private val logger = LoggerFactory.getLogger(SubscriberContext::class.java)
    }

}