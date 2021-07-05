package com.totango.multikafka

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord


class Producers {
    private val props: MutableMap<String, Any> = HashMap()

    init {
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.CLIENT_ID_CONFIG] = "producer"
        props[ProducerConfig.ACKS_CONFIG] = "all"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = IntegerSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
    }

    private val senderOptions: SenderOptions<Int, String> = SenderOptions.create(props)
    private val sender: KafkaSender<Int, String> = KafkaSender.create(senderOptions)

    fun send(topic: String, count: Int): Flux<String> {
        return sender.send(Flux.range(0,count)
            .map { k: Int ->
                SenderRecord.create(ProducerRecord(topic, k, "$k"), k)
            })
            .doOnError { e: Throwable -> logger.error("Send failed", e) }
            .map { r->
                val metadata: RecordMetadata = r.recordMetadata()
                "Message ${r.correlationMetadata()} sent successfully," +
                            " topic-partition=${metadata.topic()}-${metadata.partition()}" +
                            " offset=${metadata.offset()} timestamp=${metadata.timestamp()}\n"
            }
    }


    companion object {
        private val logger = LoggerFactory.getLogger(Producers::class.java)
    }

}