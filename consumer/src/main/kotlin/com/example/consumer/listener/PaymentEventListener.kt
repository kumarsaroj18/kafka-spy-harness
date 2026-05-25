package com.example.consumer.listener

import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

@Component
class PaymentEventListener {
    private val log = LoggerFactory.getLogger(javaClass)
    val count = AtomicInteger(0)

    @KafkaListener(topics = ["payment-events"])
    fun onMessage(message: String) {
        count.incrementAndGet()
        log.info("payment-events: {}", message.take(120))
    }
}
