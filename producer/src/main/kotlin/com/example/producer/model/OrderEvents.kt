package com.example.producer.model

data class OrderItem(val productId: String, val quantity: Int, val unitPrice: Double)

data class OrderCreatedEvent(
    val eventType: String = "ORDER_CREATED",
    val orderId: String,
    val customerId: String,
    val items: List<OrderItem>,
    val totalAmount: Double,
    val currency: String,        // USD | EUR | GBP
    val createdAt: String
)

data class OrderShippedEvent(
    val eventType: String = "ORDER_SHIPPED",
    val orderId: String,
    val trackingNumber: String,
    val carrier: String,         // FEDEX | UPS | DHL | USPS
    val shippedAt: String,
    val estimatedDelivery: String
)

data class OrderCancelledEvent(
    val eventType: String = "ORDER_CANCELLED",
    val orderId: String,
    val reason: String,          // CUSTOMER_REQUEST | OUT_OF_STOCK | PAYMENT_FAILED | FRAUD_DETECTED
    val cancelledAt: String,
    val refundAmount: Double
)
