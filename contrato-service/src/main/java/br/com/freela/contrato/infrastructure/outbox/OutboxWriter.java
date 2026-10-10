package br.com.freela.contrato.infrastructure.outbox;

import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.messaging.EventEnvelope;
import br.com.freela.contrato.infrastructure.messaging.EventEnvelopeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;

import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

@Component
public class OutboxWriter {
    private static final Logger log = LoggerFactory.getLogger(OutboxWriter.class);

    private final OutboxEventRepository repository;
    private final EventEnvelopeFactory factory;
    private final ObjectMapper objectMapper;

    public OutboxWriter(OutboxEventRepository repository, EventEnvelopeFactory factory, ObjectMapper objectMapper) {
        this.repository = repository;
        this.factory = factory;
        this.objectMapper = objectMapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void registrar(DomainEvent event) {
        String correlationId = MDC.get("correlationId");

        EventEnvelope envelope = factory.criar(event, correlationId);
        String json = objectMapper.writeValueAsString(envelope);

        repository.save(new OutboxEvent(envelope.eventId(), envelope.contratoId(),
                envelope.eventType(), json, correlationId));

        log.info("outbox.evento.registrado contratoId={} eventId={} eventType={} correlationId={}",
                envelope.contratoId(), envelope.eventId(), envelope.eventType(), correlationId);
    }
}
