package com.example.producer.model

data class UserRegisteredEvent(
    val eventType: String = "USER_REGISTERED",
    val userId: String,
    val email: String,
    val name: String,
    val country: String,         // ISO 3166-1 alpha-2
    val registeredAt: String
)

data class UserUpdatedEvent(
    val eventType: String = "USER_UPDATED",
    val userId: String,
    val changedFields: List<String>,
    val updatedAt: String
)

data class UserDeletedEvent(
    val eventType: String = "USER_DELETED",
    val userId: String,
    val reason: String,          // USER_REQUEST | GDPR_ERASURE | ADMIN_ACTION
    val deletedAt: String
)
