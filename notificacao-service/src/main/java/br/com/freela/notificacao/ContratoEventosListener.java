package br.com.freela.notificacao;

import tools.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ContratoEventosListener {
    private static final Logger log = LoggerFactory.getLogger(ContratoEventosListener.class);

    private final NotificacaoService service;
    private final ObjectMapper objectMapper;

    public ContratoEventosListener(NotificacaoService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${freela.kafka.topico-eventos}", groupId = "${spring.application.name}", concurrency = "3")
    public void receber(ConsumerRecord<String, String> record) {
        EventEnvelope envelope;
        try {
            envelope = objectMapper.readValue(record.value(), EventEnvelope.class);
        } catch (RuntimeException e) {
            log.error("notificacao.evento.invalido chave={} partition={} offset={} erro={}",
                    record.key(), record.partition(), record.offset(), e.toString());
            throw e;
        }

        log.info("notificacao.evento.recebido eventId={} eventType={} contratoId={} correlationId={} partition={} offset={}",
                envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId(),
                record.partition(), record.offset());

        try {
            var resultado = service.processar(envelope);
            log.info("notificacao.evento.concluido eventId={} eventType={} contratoId={} correlationId={} resultado={}",
                    envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId(), resultado);
        } catch (DataIntegrityViolationException e) {
            log.warn("notificacao.evento.duplicado.concorrente eventId={} eventType={} contratoId={} correlationId={}",
                    envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId());
        } catch (RuntimeException e) {
            log.error("notificacao.evento.falha eventId={} eventType={} contratoId={} correlationId={} erro={}",
                    envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId(), e.toString());
            throw e; // a estratégia de retry e DLT entra na Etapa 5
        }
    }
}
