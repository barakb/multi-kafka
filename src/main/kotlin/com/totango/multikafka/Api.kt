package com.totango.multikafka

import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.*
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Mono.delay
import reactor.core.publisher.Sinks
import java.time.Duration


@RestController
@RequestMapping("/api")
class Api {

    private val producers = Producers()
    private val subscribers = Subscribers("my-group")
    private val sink: Sinks.Many<Flux<Buffer>> = Sinks.many().unicast().onBackpressureBuffer()
    @Suppress("unused")
    private val disposable: Disposable = sink.asFlux().flatMap({it}, 1, 1 )
        .transform (::processBuffer)
        .subscribe()

    fun processBuffer(stream: Flux<Buffer>) : Flux<Buffer>{
        return stream.flatMap({ buffer ->
            logger.info("--> processing buffer $buffer started")
            delay(Duration.ofSeconds(3)).then(Mono.fromCallable {
                logger.info("<-- processing buffer $buffer ended")
                buffer
            })
        }, 10, 1)
    }

    @PostMapping("/{topic}/send")
    fun write(@PathVariable topic: String
              , @RequestParam(required = false, defaultValue = "30") count: Int
    ) : Flux<String> {
        logger.info("writing $count to $topic")
        return producers.send(topic, count)
    }

    @PostMapping("/{topic}/subscribe")
    suspend fun subscribe(@PathVariable topic: String): Sinks.EmitResult {
        return sink.tryEmitNext(subscribers.subscribe(topic))
    }

    @PostMapping("/{topic}/unsubscribe")
    suspend fun unSubscribe(@PathVariable topic: String){
        subscribers.unsubscribe(topic)
    }

    fun range(start: Int, count: Int) : Flux<List<Int>>{
        return Flux.interval(Duration.ofSeconds(1))
            .zipWith(Flux.range(start, count))
            .map {
                it.t2
            }.bufferTimeout(10, Duration.ofSeconds(2))
    }

    @PostMapping("/test")
    fun test(){
        Flux.fromIterable(listOf(range(0, 1000),range(1000, 1000), range(2000, 1000)))
            .flatMap({it}, 5, 1)
            .map{
                delay(Duration.ofSeconds(1))
                it
            }
            .doOnNext{logger.info("onNext $it")}

            .subscribe()
    }


    companion object{
        private val logger = LoggerFactory.getLogger(Api::class.java)
    }
}
