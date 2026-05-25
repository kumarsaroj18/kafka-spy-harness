package com.example.producer.model

data class PaymentInitiatedEvent(
    val eventType: String = "PAYMENT_INITIATED",
    val paymentId: String,
    val orderId: String,
    val amount: Double,
    val currency: String,        // USD | EUR | GBP
    val method: String,          // CREDIT_CARD | DEBIT_CARD | PAYPAL | BANK_TRANSFER
    val initiatedAt: String
)

data class PaymentCompletedEvent(
    val eventType: String = "PAYMENT_COMPLETED",
    val paymentId: String,
    val transactionId: String,
    val amount: Double,
    val completedAt: String
)

data class PaymentFailedEvent(
    val eventType: String = "PAYMENT_FAILED",
    val paymentId: String,
    val errorCode: String,       // INSUFFICIENT_FUNDS | CARD_DECLINED | TIMEOUT | FRAUD_BLOCKED
    val errorMessage: String,
    val failedAt: String
)
