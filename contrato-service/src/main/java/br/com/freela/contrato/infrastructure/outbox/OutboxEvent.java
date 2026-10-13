package br.com.freela.contrato.infrastructure.outbox;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;


@Entity
@Table(name = "outbox_event", indexes = {
        @Index(name = "idx_outbox_pendentes", columnList = "publishedAt, id")
})
public class OutboxEvent {

    // id sequencial: define a ordem de publicação (mais confiável que o timestamp)
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private UUID eventId;

    @Column(nullable = false)
    private UUID aggregateId;

    @Column(nullable = false, length = 80)
    private String eventType;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(length = 100)
    private String correlationId ;
    
    @Column(length = 64)
    private String traceId;

    @Column(length = 32)
    private String spanId;

    @Column(nullable = false)
    private Instant createdAt;

    private Instant publishedAt;

    protected OutboxEvent() {}

    public OutboxEvent(UUID eventId, UUID aggregateId, String eventType, String payload,
                       String correlationId, String traceId, String spanId) {
        this.eventId = eventId;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.correlationId = correlationId;
        this.traceId = traceId;
        this.spanId = spanId;
        this.createdAt = Instant.now();
    }

    public void marcarComoPublicado() { this.publishedAt = Instant.now(); }

    public Long getId() { return id; }
    public UUID getEventId() { return eventId; }
    public UUID getAggregateId() { return aggregateId; }
    public String getEventType() { return eventType; }
    public String getPayload() { return payload; }
    public String getCorrelationId() { return correlationId; }
    public String getTraceId() { return traceId; }
    public String getSpanId() { return spanId; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getPublishedAt() { return publishedAt; }
}