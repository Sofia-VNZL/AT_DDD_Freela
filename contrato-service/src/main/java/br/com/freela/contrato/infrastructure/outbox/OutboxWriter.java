package br.com.freela.contrato.infrastructure.outbox;

import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.messaging.EventEnvelope;
import br.com.freela.contrato.infrastructure.messaging.EventEnvelopeFactory;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.slf4j.*;
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
    private final Tracer tracer;

    public OutboxWriter(OutboxEventRepository repository, EventEnvelopeFactory factory,
                        ObjectMapper objectMapper, Tracer tracer) {
        this.repository = repository;
        this.factory = factory;
        this.objectMapper = objectMapper;
        this.tracer = tracer;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void registrar(DomainEvent event) {
        String correlationId = MDC.get("correlationId");
        Span spanAtual = tracer.currentSpan();
        String traceId = spanAtual != null ? spanAtual.context().traceId() : null;
        String spanId = spanAtual != null ? spanAtual.context().spanId() : null;

        EventEnvelope envelope = factory.criar(event, correlationId);
        String json = objectMapper.writeValueAsString(envelope);

        repository.save(new OutboxEvent(envelope.eventId(), envelope.contratoId(),
                envelope.eventType(), json, correlationId, traceId, spanId));

        log.info("outbox.evento.registrado contratoId={} eventId={} eventType={} correlationId={} traceId={}",
                envelope.contratoId(), envelope.eventId(), envelope.eventType(), correlationId, traceId);
    }
}
