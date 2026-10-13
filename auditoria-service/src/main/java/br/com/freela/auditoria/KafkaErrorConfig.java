package br.com.freela.auditoria;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.core.JacksonException;


@Configuration
public class KafkaErrorConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaErrorConfig.class);

    @Bean
    public NewTopic topicoDlt(@Value("${freela.kafka.topico-dlt:contratos.eventos.dlt}") String nome) {
        return TopicBuilder.name(nome).partitions(3).replicas(1).build();
    }

    @Bean
    public CommonErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> template,
                                                @Value("${freela.kafka.topico-dlt:contratos.eventos.dlt}") String topicoDlt) {

        var publicador = new DeadLetterPublishingRecoverer(template, (record, ex) -> new TopicPartition(topicoDlt, -1));

        ConsumerRecordRecoverer recoverer = (record, ex) -> {
            log.error("auditoria.evento.enviado_dlt chave={} topicoOrigem={} partition={} offset={} topicoDlt={} erro={}",
                    record.key(), record.topic(), record.partition(), record.offset(), topicoDlt, ex.toString());
            publicador.accept(record, ex);
        };

        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2L));

        handler.addNotRetryableExceptions(JacksonException.class, IllegalArgumentException.class, NullPointerException.class);
        return handler;
    }
}
