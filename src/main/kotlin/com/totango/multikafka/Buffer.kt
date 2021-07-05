package com.totango.multikafka

import reactor.kafka.receiver.ReceiverRecord

data class Buffer(val topic:String, val payload: List<ReceiverRecord<Int, String>>)