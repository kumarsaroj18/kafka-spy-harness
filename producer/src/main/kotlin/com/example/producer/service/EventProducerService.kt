package com.example.producer.service

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class EventProducerService(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper
) {
    companion object {
        private val TOPIC_MAP = mapOf(
            "ORDER_CREATED"       to "order-events",
            "ORDER_SHIPPED"       to "order-events",
            "ORDER_CANCELLED"     to "order-events",
            "PAYMENT_INITIATED"   to "payment-events",
            "PAYMENT_COMPLETED"   to "payment-events",
            "PAYMENT_FAILED"      to "payment-events",
            "USER_REGISTERED"     to "user-events",
            "USER_UPDATED"        to "user-events",
            "USER_DELETED"        to "user-events",
        )
    }

    fun publish(eventType: String, event: JsonNode) {
        val topic = TOPIC_MAP[eventType]
            ?: throw IllegalArgumentException("Unknown eventType: $eventType")
        kafkaTemplate.send(topic, eventType, objectMapper.writeValueAsString(event))
    }
}
