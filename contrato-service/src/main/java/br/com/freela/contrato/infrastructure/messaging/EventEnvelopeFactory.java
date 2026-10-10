package br.com.freela.contrato.infrastructure.messaging;

import br.com.freela.contrato.domain.event.ContratoCancelado;
import br.com.freela.contrato.domain.event.ContratoConcluido;
import br.com.freela.contrato.domain.event.ContratoCriado;
import br.com.freela.contrato.domain.event.EntregaRegistrada;
import br.com.freela.contrato.domain.shared.DomainEvent;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class EventEnvelopeFactory {
    public EventEnvelope criar(DomainEvent event, String correlationId) {
        return switch (event) {
            case ContratoCriado e -> envelope(e, e.contratoId(), correlationId, payload(
                    "clienteId", e.clienteId(), "freelancerId", e.freelancerId(),
                    "titulo", e.titulo(), "valor", e.valor()));
            case EntregaRegistrada e -> envelope(e, e.contratoId(), correlationId, payload(
                    "clienteId", e.clienteId(), "freelancerId", e.freelancerId()));
            case ContratoConcluido e -> envelope(e, e.contratoId(), correlationId, payload(
                    "clienteId", e.clienteId(), "freelancerId", e.freelancerId(),
                    "valor", e.valor()));
            case ContratoCancelado e -> envelope(e, e.contratoId(), correlationId, payload(
                    "clienteId", e.clienteId(), "freelancerId", e.freelancerId(),
                    "statusAnterior", e.statusAnterior().name()));
            default -> throw new IllegalArgumentException("Evento sem mapeamento: " + event.eventType());
        };
    }

    private EventEnvelope envelope(DomainEvent e, java.util.UUID contratoId, String correlationId, Map<String, Object> payload) {
        return new EventEnvelope(e.eventId(), e.eventType(), EventEnvelope.VERSAO_ATUAL,
                contratoId, e.occurredAt(), correlationId, payload);
    }

    private Map<String, Object> payload(Object... paresChaveValor) {
        var map = new LinkedHashMap<String, Object>();
        for (int i = 0; i < paresChaveValor.length; i += 2) {
            map.put((String) paresChaveValor[i], paresChaveValor[i + 1]);
        }
        return map;
    }
}

