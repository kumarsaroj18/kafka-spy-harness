package com.example.producer.controller

import com.example.producer.service.EventProducerService
import com.fasterxml.jackson.databind.JsonNode
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/events")
class EventController(private val service: EventProducerService) {

    @PostMapping
    fun publish(@RequestBody event: JsonNode): Map<String, String> {
        val eventType = event.get("eventType")?.asText()
            ?: return mapOf("status" to "error", "message" to "missing eventType field")
        service.publish(eventType, event)
        return mapOf("status" to "ok", "eventType" to eventType)
    }

    @PostMapping("/bulk")
    fun publishBulk(@RequestBody events: List<JsonNode>): Map<String, Any> {
        var count = 0
        events.forEach { event ->
            val eventType = event.get("eventType")?.asText() ?: return@forEach
            service.publish(eventType, event)
            count++
        }
        return mapOf("status" to "ok", "published" to count)
    }
}
