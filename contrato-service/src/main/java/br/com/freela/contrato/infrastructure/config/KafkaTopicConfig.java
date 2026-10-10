package br.com.freela.contrato.infrastructure.config;


import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    @Bean
    public NewTopic topicoContratosEventos(@Value("${freela.kafka.topico-eventos}") String nome) {
        return TopicBuilder.name(nome).partitions(3).replicas(1).build();
    }
}
