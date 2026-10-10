package br.com.freela.contrato.infrastructure.outbox;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_event", indexes = {
        @Index(name = "idx_outbox_pendentes", columnList = "publishedAt, id")
})
public class OutboxEvent {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private UUID eventId;

    @Column(nullable = false)
    private UUID aggregateId;

    @Column(nullable = false, length = 80)
    private String eventType;

    @Column(nullable = false, columnDefinition = "text" )
    private String payload;

    @Column(length = 100)
    private String correlationId ;

    @Column(nullable = false)
    private Instant createdAt;

    private Instant publishedAt;

    protected OutboxEvent() {}

    public OutboxEvent(UUID eventId, UUID aggregateId, String eventType, String payload, String correlationId) {
        this.eventId = eventId;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
        this.correlationId = correlationId;
        this.createdAt = Instant.now();
    }

    public void marcarComoPublicado() { this.publishedAt = Instant.now(); }

    public Long getId() { return id; }
    public UUID getEventId() { return eventId; }
    public UUID getAggregateId() { return aggregateId; }
    public String getEventType() { return eventType; }
    public String getPayload() { return payload; }
    public String getCorrelationId() { return correlationId; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getPublishedAt() { return publishedAt; }
}


