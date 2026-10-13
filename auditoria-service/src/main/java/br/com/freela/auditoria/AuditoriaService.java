package br.com.freela.auditoria;

import org.slf4j.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AuditoriaService {
 private static final Logger log = LoggerFactory.getLogger(AuditoriaService.class);

 public enum Resultado { REGISTRADO, DUPLICADO }

 private final EventoAuditoriaRepository repository;

 public AuditoriaService(EventoAuditoriaRepository repository) {
  this.repository = repository;
 }

 @Transactional
 public Resultado registrar(EventEnvelope envelope, String conteudo, int partition, long offset) {

  if (repository.existsById(envelope.eventId())) {
   log.info("auditoria.evento.duplicado eventId={} eventType={} contratoId={} correlationId={}",
           envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId());
   return Resultado.DUPLICADO;
  }

  log.info("auditoria.registro.inicio eventId={} contratoId={} eventType={} correlationId={}",
          envelope.eventId(), envelope.contratoId(), envelope.eventType(), envelope.correlationId());

  repository.saveAndFlush(new EventoAuditoria(envelope, conteudo, partition, offset));

  log.info("auditoria.registro.sucesso eventId={} contratoId={} eventType={} correlationId={} partition={} offset={}",
          envelope.eventId(), envelope.contratoId(), envelope.eventType(), envelope.correlationId(), partition, offset);
  return Resultado.REGISTRADO;
 }
}
