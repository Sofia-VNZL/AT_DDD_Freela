package br.com.freela.auditoria;

import org.slf4j.*;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.*;
import java.util.*;

@RestController
@RequestMapping("/api/auditoria")
public class AuditoriaController {
 private static final Logger log = LoggerFactory.getLogger(AuditoriaController.class);

 private final EventoAuditoriaRepository repository;

 AuditoriaController(EventoAuditoriaRepository repository) {
  this.repository = repository;
 }

 @GetMapping
 public List<EventoAuditoria> listar(
         @RequestHeader(value = "X-Correlation-Id", required = false) String correlationIdRequisicao,
         @RequestParam(value = "contratoId", required = false) UUID contratoId,
         @RequestParam(value = "eventId", required = false) UUID eventId,
         @RequestParam(value = "correlationId", required = false) String correlationId) {

  log.info("http.auditoria.listar correlationId={} filtroContratoId={} filtroEventId={} filtroCorrelationId={}",
          correlationIdRequisicao, contratoId, eventId, correlationId);

  if (eventId != null) {
   return repository.findById(eventId).map(List::of).orElse(List.of());
  }
  if (contratoId != null) {
   return repository.findByContratoIdOrderByOccurredAtAscKafkaOffsetAsc(contratoId);
  }
  if (correlationId != null) {
   return repository.findByCorrelationIdOrderByOccurredAtAsc(correlationId);
  }
  return repository.findAll(Sort.by("occurredAt"));
 }
}
