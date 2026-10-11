package br.com.freela.reputacao;

import org.slf4j.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.util.UUID;
import java.math.RoundingMode;

@Service
public class ReputacaoService {

 private static final Logger log=LoggerFactory.getLogger(ReputacaoService.class);

 public enum Resultado { PROCESSADO, DUPLICADO, IGNORADO }

 private final ReputacaoRepository repository;
 private final EventoProcessadoRepository eventosProcessados;

 public ReputacaoService(ReputacaoRepository repository, EventoProcessadoRepository eventosProcessados) {
  this.repository = repository;
  this.eventosProcessados = eventosProcessados;
 }

 @Transactional
 public Resultado processar(EventEnvelope envelope) {
  if (!"ContratoConcluido".equals(envelope.eventType())) {
   log.info("reputacao.evento.ignorado eventId={} eventType={} contratoId={} correlationId={}",
           envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId());
   return Resultado.IGNORADO;
  }

  if (eventosProcessados.existsById(envelope.eventId())) {
   log.info("reputacao.evento.duplicado eventId={} eventType={} contratoId={} correlationId={}",
           envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId());
   return Resultado.DUPLICADO;
  }

  UUID freelancerId = UUID.fromString(envelope.payload().get("freelancerId").toString());
  BigDecimal valor = new BigDecimal(envelope.payload().get("valor").toString()).setScale(2, RoundingMode.HALF_UP);

  log.info("reputacao.atualizacao.inicio eventId={} contratoId={} freelancerId={} valor={} correlationId={}",
          envelope.eventId(), envelope.contratoId(), freelancerId, valor, envelope.correlationId());

  var reputacao = repository.findById(freelancerId).orElseGet(() -> new ReputacaoFreelancer(freelancerId));
  reputacao.registrarContrato(valor);
  repository.saveAndFlush(reputacao);

  eventosProcessados.saveAndFlush(new EventoProcessado(envelope.eventId(), envelope.eventType(), envelope.contratoId()));

  log.info("reputacao.atualizacao.sucesso eventId={} contratoId={} freelancerId={} contratosConcluidos={} valorTotal={} correlationId={}",
          envelope.eventId(), envelope.contratoId(), freelancerId,
          reputacao.getContratosConcluidos(), reputacao.getValorTotal(), envelope.correlationId());
  return Resultado.PROCESSADO;
 }
}
