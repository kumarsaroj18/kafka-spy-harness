package com.example.consumer.controller

import com.example.consumer.listener.InferredEventListener
import com.example.consumer.listener.OrderEventListener
import com.example.consumer.listener.PaymentEventListener
import com.example.consumer.listener.ShapeEventListener
import com.example.consumer.listener.UntypedEventListener
import com.example.consumer.listener.UserEventListener
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/status")
class StatusController(
    private val orderListener:    OrderEventListener,
    private val paymentListener:  PaymentEventListener,
    private val userListener:     UserEventListener,
    private val inferredListener: InferredEventListener,
    private val untypedListener:  UntypedEventListener,
    private val shapeListener:    ShapeEventListener,
) {
    @GetMapping
    fun status() = mapOf(
        "status" to "up",
        "received" to mapOf(
            "order-events"    to orderListener.count.get(),
            "payment-events"  to paymentListener.count.get(),
            "user-events"     to userListener.count.get(),
            "inferred-events" to inferredListener.count.get(),
            "untyped-events"  to untypedListener.count.get(),
            "shape-events"    to shapeListener.count.get(),
        )
    )
}
