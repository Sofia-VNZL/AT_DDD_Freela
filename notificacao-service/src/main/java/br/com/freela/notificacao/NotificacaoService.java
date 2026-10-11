package br.com.freela.notificacao;

import org.slf4j.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class NotificacaoService {

  //private static final Logger log=LoggerFactory.getLogger(NotificacaoService.class); private final NotificacaoRepository repository;
  private static final Logger log = LoggerFactory.getLogger(NotificacaoService.class);

  public enum Resultado { PROCESSADO, DUPLICADO, IGNORADO }

  private record Aviso(UUID destinatarioId, String tipo, String mensagem) {}

  private final NotificacaoRepository repository;
  private final EventoProcessadoRepository eventosProcessados;

 //public NotificacaoService(NotificacaoRepository r){this.repository=r;}
  public NotificacaoService(NotificacaoRepository repository, EventoProcessadoRepository eventosProcessados) {
   this.repository = repository;
   this.eventosProcessados = eventosProcessados;
  }


  @Transactional
  //public void registrar(UUID contratoId, UUID destinatarioId, String tipo, String mensagem)
  public Resultado processar(EventEnvelope envelope){
   if (eventosProcessados.existsById(envelope.eventId())) {
    log.info("notificacao.evento.duplicado eventId={} eventType={} contratoId={} correlationId={}",
            envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId());
    return Resultado.DUPLICADO;
   }

   List<Aviso> avisos = montarAvisos(envelope);
   if (avisos.isEmpty()) {
    log.info("notificacao.evento.ignorado eventId={} eventType={} contratoId={} correlationId={}",
            envelope.eventId(), envelope.eventType(), envelope.contratoId(), envelope.correlationId());
    return Resultado.IGNORADO;
   }

   for (Aviso aviso : avisos) {
    var notificacao = repository.save(
            new Notificacao(envelope.contratoId(), aviso.destinatarioId(), aviso.tipo(), aviso.mensagem()));
    log.info("notificacao.registro.sucesso notificacaoId={} eventId={} contratoId={} destinatarioId={} tipo={} correlationId={}",
            notificacao.getId(), envelope.eventId(), envelope.contratoId(),
            aviso.destinatarioId(), aviso.tipo(), envelope.correlationId());
   }


   eventosProcessados.saveAndFlush(new EventoProcessado(envelope.eventId(), envelope.eventType(), envelope.contratoId()));
   return Resultado.PROCESSADO;
  }

 private List<Aviso> montarAvisos(EventEnvelope envelope) {
  UUID clienteId = uuid(envelope, "clienteId");
  UUID freelancerId = uuid(envelope, "freelancerId");

  return switch (envelope.eventType()) {
   case "ContratoCriado" -> List.of(new Aviso(freelancerId, "NOVO_CONTRATO",
           "Você recebeu um novo contrato: " + titulo(envelope)));
   case "EntregaRegistrada" -> List.of(new Aviso(clienteId, "ENTREGA_REGISTRADA",
           "O freelancer registrou a entrega do contrato. Verifique o resultado."));
   case "ContratoConcluido" -> List.of(new Aviso(freelancerId, "CONTRATO_CONCLUIDO",
           "Contrato concluído com sucesso."));
   case "ContratoCancelado" -> List.of(
           new Aviso(clienteId, "CONTRATO_CANCELADO", "O contrato foi cancelado."),
           new Aviso(freelancerId, "CONTRATO_CANCELADO", "O contrato foi cancelado."));
   default -> List.of();
  };
 }

 private UUID uuid(EventEnvelope envelope, String campo) {
  Object valor = envelope.payload() == null ? null : envelope.payload().get(campo);
  return valor == null ? null : UUID.fromString(valor.toString());
 }

 private String titulo(EventEnvelope envelope) {
  Object titulo = envelope.payload().get("titulo");
  String texto = titulo == null ? "(sem título)" : titulo.toString();
  return texto.length() > 300 ? texto.substring(0, 300) : texto;
 }
}
