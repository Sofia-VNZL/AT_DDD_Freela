package br.com.freela.auditoria;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

interface EventoAuditoriaRepository extends JpaRepository<EventoAuditoria, UUID>{

    List<EventoAuditoria> findByContratoIdOrderByOccurredAtAscKafkaOffsetAsc(UUID contratoId);

    List<EventoAuditoria> findByCorrelationIdOrderByOccurredAtAsc(String correlationId);
}
