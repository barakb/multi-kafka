package com.totango.multikafka

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class MultiKafkaApplication

fun main(args: Array<String>) {
	runApplication<MultiKafkaApplication>(*args)
}
