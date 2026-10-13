package br.com.freela.auditoria;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "eventos_auditados", indexes = {
        @Index(name= "idx_auditoria_contrato", columnList= "contratoId"),
        @Index(name= "idx_auditoria_correlacao", columnList= "correlationId")
})
class EventoAuditoria {
 @Id
 private UUID eventId;

 @Column(nullable = false, length = 80)
 private String eventType;

 @Column(nullable = false)
 private UUID contratoId;

 @Column(nullable = false)
 private Instant occurredAt;

 @Column(length = 100)
 private String correlationId;

 @Column(nullable = false, columnDefinition= "text")
 private String conteudo;

 @Column(nullable = false)
 private Instant recebidoEm;


 private int kafkaPartition;
 private long kafkaOffset;

 protected EventoAuditoria() {
 }

 EventoAuditoria(EventEnvelope envelope, String conteudo, int kafkaPartition, long kafkaOffset) {
  this.eventId = envelope.eventId();
  this.eventType = envelope.eventType();
  this.contratoId = envelope.contratoId();
  this.occurredAt = envelope.occurredAt();
  this.correlationId = envelope.correlationId();
  this.conteudo = conteudo;
  this.recebidoEm = Instant.now();
  this.kafkaPartition = kafkaPartition;
  this.kafkaOffset = kafkaOffset;
 }

 public UUID getEventId() {
  return eventId;
 }

 public String getEventType() {
  return eventType;
 }

 public UUID getContratoId() {
  return contratoId;
 }

 public Instant getOccurredAt() {
  return occurredAt;
 }

 public String getCorrelationId() {
  return correlationId;
 }

 public String getConteudo() {
  return conteudo;
 }

 public Instant getRecebidoEm() {
  return recebidoEm;
 }

 public int getKafkaPartition() {
  return kafkaPartition;
 }

 public long getKafkaOffset() {
  return kafkaOffset;
 }
}
