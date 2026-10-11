package br.com.freela.reputacao;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import tools.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class ContratoEventosListener {
    private static final Logger log = LoggerFactory.getLogger(ContratoEventosListener.class);
    private static final int MAX_TENTATIVAS = 3;

    private final ReputacaoService service;
    private final ObjectMapper objectMapper;

    public ContratoEventosListener(ReputacaoService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;
    }


    @KafkaListener(topics = "${freela.kafka.topico-eventos}", groupId = "${spring.application.name}", concurrency = "3")
    public void receber(ConsumerRecord<String, String> record) {
        EventEnvelope envelope;
        try {
            envelope = objectMapper.readValue(record.value(), EventEnvelope.class);
        } catch (RuntimeException e) {
            log.error("reputacao.evento.invalido chave={} partition={} offset={} erro={}",
                    record.key(), record.partition(), record.offset(), e.toString());
            throw e;
        }

        log.info("reputacao.evento.recebido eventId={} eventType={} contratoId={} correlationId={} partition={} offset={}",
                envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId(),
                record.partition(), record.offset());

        try {
            var resultado = processarComTentativas(envelope);
            log.info("reputacao.evento.concluido eventId={} eventType={} contratoId={} correlationId={} resultado={}",
                    envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId(), resultado);
        } catch (RuntimeException e) {
            log.error("reputacao.evento.falha eventId={} eventType={} contratoId={} correlationId={} erro={}",
                    envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId(), e.toString());
            throw e;
        }
    }


    private ReputacaoService.Resultado processarComTentativas(EventEnvelope envelope) {
        for (int tentativa = 1; ; tentativa++) {
            try {
                return service.processar(envelope);
            } catch (ObjectOptimisticLockingFailureException | DataIntegrityViolationException e) {
                if (tentativa >= MAX_TENTATIVAS) {
                    throw e;
                }
                log.warn("reputacao.evento.conflito eventId={} contratoId={} correlationId={} tentativa={}",
                        envelope.eventId(), envelope.contratoId(), envelope.correlationId(), tentativa);
            }
        }
    }
}
