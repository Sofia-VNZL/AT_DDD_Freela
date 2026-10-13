package br.com.freela.auditoria;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class ContratoEventosListener {
    private static final Logger log = LoggerFactory.getLogger(ContratoEventosListener.class);

    private final AuditoriaService service;
    private final ObjectMapper objectMapper;

    public ContratoEventosListener(AuditoriaService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${freela.kafka.topico-eventos}", groupId = "${spring.application.name}", concurrency = "3")
    public void receber(ConsumerRecord<String, String> record) {
        EventEnvelope envelope;
        try {
            envelope = objectMapper.readValue(record.value(), EventEnvelope.class);
        } catch (RuntimeException e) {
            log.error("auditoria.evento.invalido chave={} partition={} offset={} erro={}",
                    record.key(), record.partition(), record.offset(), e.toString());
            throw e;
        }

        log.info("auditoria.evento.recebido eventId={} eventType={} contratoId={} correlationId={} partition={} offset={}",
                envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId(),
                record.partition(), record.offset());

        try {
            var resultado = service.registrar(envelope, record.value(), record.partition(), record.offset());
            log.info("auditoria.evento.concluido eventId={} eventType={} contratoId={} correlationId={} resultado={}",
                    envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId(), resultado);
        } catch (DataIntegrityViolationException e) {
            log.warn("auditoria.evento.duplicado.concorrente eventId={} eventType={} contratoId={} correlationId={}",
                    envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId());
        } catch (RuntimeException e) {
            log.error("auditoria.evento.falha eventId={} eventType={} contratoId={} correlationId={} erro={}",
                    envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId(), e.toString());
            throw e;
        }
    }
}
