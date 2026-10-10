package br.com.freela.contrato.infrastructure.outbox;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topico;

    public OutboxPublisher(OutboxEventRepository repository,
                           KafkaTemplate<String, String> kafkaTemplate,
                           @Value("${freela.kafka.topico-eventos}") String topico) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.topico = topico;
    }

    @Scheduled(fixedDelayString = "${freela.outbox.intervalo-ms:1000}")
    @Transactional
    public void publicarPendentes() {
        List<OutboxEvent> pendentes = repository.findTop100ByPublishedAtIsNullOrderByIdAsc();
        if (pendentes.isEmpty()) return;

        log.info("outbox.publicacao.inicio pendentes={}", pendentes.size());

        for (OutboxEvent evento : pendentes) {
            try {
                var record = new ProducerRecord<String, String>(topico, evento.getAggregateId().toString(), evento.getPayload());
                record.headers().add("eventId", evento.getEventId().toString().getBytes(StandardCharsets.UTF_8));
                record.headers().add("eventType", evento.getEventType().getBytes(StandardCharsets.UTF_8));
                if (evento.getCorrelationId() != null) {
                    record.headers().add("correlationId", evento.getCorrelationId().getBytes(StandardCharsets.UTF_8));
                }

                var resultado = kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);

                evento.marcarComoPublicado();
                log.info("outbox.evento.publicado contratoId={} eventId={} eventType={} correlationId={} topic={} partition={} offset={}",
                        evento.getAggregateId(), evento.getEventId(), evento.getEventType(), evento.getCorrelationId(),
                        topico, resultado.getRecordMetadata().partition(), resultado.getRecordMetadata().offset());
            } catch (Exception e) {
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                log.error("outbox.evento.falha contratoId={} eventId={} eventType={} correlationId={} erro={}",
                        evento.getAggregateId(), evento.getEventId(), evento.getEventType(), evento.getCorrelationId(), e.toString());
                break;
            }
        }
    }
}
