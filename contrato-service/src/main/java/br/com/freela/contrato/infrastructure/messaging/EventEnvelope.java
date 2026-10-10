package br.com.freela.contrato.infrastructure.messaging;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record EventEnvelope(UUID eventId,
                            String eventType,
                            int version,
                            UUID contratoId,
                            Instant occurredAt,
                            String correlationId,
                            Map<String, Object> payload) {

    public static final int VERSAO_ATUAL = 1;
}
