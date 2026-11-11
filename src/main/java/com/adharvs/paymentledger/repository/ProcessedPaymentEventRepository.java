package com.adharvs.paymentledger.repository;

import com.adharvs.paymentledger.domain.ProcessedPaymentEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ProcessedPaymentEventRepository extends JpaRepository<ProcessedPaymentEvent, UUID> {
}
