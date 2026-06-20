package com.example.consumer.listener

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

@Component
class InferredEventListener {
    private val log = LoggerFactory.getLogger(javaClass)
    private val mapper = ObjectMapper()
    val count = AtomicInteger(0)

    @KafkaListener(topics = ["inferred-events"])
    fun onMessage(message: String) {
        count.incrementAndGet()
        try {
            val node = mapper.readTree(message)
            log.info("inferred-events action={}: {}", node.get("action")?.asText(), message.take(120))
        } catch (_: Exception) {
            log.info("inferred-events: {}", message.take(120))
        }
    }
}
