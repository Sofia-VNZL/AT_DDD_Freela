package br.com.freela.contrato.application;

import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.outbox.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

@Service
public class ContratoApplicationService {
    private static final Logger log = LoggerFactory.getLogger(ContratoApplicationService.class);
    private final ContratoRepository repository;
    private final OutboxWriter outboxWriter;

    //public ContratoApplicationService(ContratoRepository repository) { this.repository = repository; }
    public ContratoApplicationService(ContratoRepository repository, OutboxWriter outboxWriter) {
        this.repository = repository;
        this.outboxWriter = outboxWriter;
    }

    @Transactional
    public Contrato criar(CriarContratoCommand cmd) {
        log.info("contrato.criacao.inicio clienteId={} freelancerId={} titulo={} valor={}",
                cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        Contrato contrato = Contrato.criar(cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        log.info("contrato.dominio.criado contratoId={} status={} domainEvents={}",
                contrato.id(), contrato.status(), contrato.domainEvents().size());
        Contrato salvo = repository.salvar(contrato);

        // PONTO DO ASSESSMENT:
        // Os eventos existem no Aggregate, mas ainda NÃO são publicados no Kafka.
        // O aluno deverá implementar a estratégia de publicação/mensagens transacionais.

        //registrarEventosPendentes(contrato);
        registrarEventosNaOutbox(contrato);

        log.info("contrato.criacao.sucesso contratoId={} clienteId={} freelancerId={} status={}",
                salvo.id(), salvo.clienteId(), salvo.freelancerId(), salvo.status());
        return salvo;
    }

    @Transactional
    public Contrato registrarEntrega(UUID id) {
        return executar(id, "entrega", Contrato::registrarEntrega);
    }

    @Transactional
    public Contrato concluir(UUID id) {
        return executar(id, "conclusao", Contrato::concluir);
    }

    @Transactional
    public Contrato cancelar(UUID id) {
        return executar(id, "cancelamento", Contrato::cancelar);
    }

    @Transactional(readOnly = true)
    public Contrato buscar(UUID id) {
        log.info("contrato.busca.inicio contratoId={}", id);
        var contrato = repository.buscarPorId(id).orElseThrow(() -> new IllegalArgumentException("Contrato não encontrado: " + id));
        log.info("contrato.busca.sucesso contratoId={} status={}", id, contrato.status());
        return contrato;
    }

    @Transactional(readOnly = true)
    public List<Contrato> listar() {
        log.info("contrato.listagem.inicio");
        var contratos = repository.listar();
        log.info("contrato.listagem.sucesso quantidade={}", contratos.size());
        return contratos;
    }

    //-----------------------------Revisar
    private Contrato executar(UUID id, String operacao, Consumer<Contrato> acao) {
        log.info("contrato.{}.inicio contratoId={}", operacao, id);
        Contrato contrato = repository.buscarPorId(id)
                .orElseThrow(() -> new IllegalArgumentException("Contrato não encontrado: " + id));
        String statusAnterior = contrato.status().name();

        acao.accept(contrato);
        log.info("contrato.{}.dominio contratoId={} statusAnterior={} statusAtual={} domainEvents={}",
                operacao, id, statusAnterior, contrato.status(), contrato.domainEvents().size());

        Contrato salvo = repository.salvar(contrato);
        //registrarEventosPendentes(contrato);
        registrarEventosNaOutbox(contrato);

        log.info("contrato.{}.sucesso contratoId={} status={}", operacao, salvo.id(), salvo.status());
        return salvo;
    }

    // PONTO DO ASSESSMENT (Etapa 2): revisar
    private void registrarEventosNaOutbox(Contrato contrato) {
        for (DomainEvent event : contrato.pullDomainEvents()) {
            outboxWriter.registrar(event);
        }
    }
}
