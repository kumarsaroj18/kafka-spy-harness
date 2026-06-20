package com.example.consumer.listener

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

@Component
class ShapeEventListener {
    private val log = LoggerFactory.getLogger(javaClass)
    private val mapper = ObjectMapper()
    val count = AtomicInteger(0)

    @KafkaListener(topics = ["shape-events"])
    fun onMessage(message: String) {
        count.incrementAndGet()
        try {
            val node = mapper.readTree(message)
            val shape = when {
                node.has("cardNumber")    -> "card-payment"
                node.has("routingNumber") -> "bank-transfer"
                else                      -> "unknown"
            }
            log.info("shape-events shape={}: {}", shape, message.take(120))
        } catch (_: Exception) {
            log.info("shape-events: {}", message.take(120))
        }
    }
}
