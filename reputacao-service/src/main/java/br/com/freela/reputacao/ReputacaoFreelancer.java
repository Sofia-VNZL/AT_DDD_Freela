package br.com.freela.reputacao;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import java.math.BigDecimal;
import java.util.UUID;

@Entity
@Table(name="reputacoes")
class ReputacaoFreelancer {

 @Id
 UUID freelancerId;
 int contratosConcluidos;
 BigDecimal valorTotal=BigDecimal.ZERO;

 @Version
 private Long version;

 protected ReputacaoFreelancer() {}

 ReputacaoFreelancer(UUID freelancerId) {
  this.freelancerId = freelancerId;
 }

 void registrarContrato(BigDecimal valor) {
  this.contratosConcluidos++;
  this.valorTotal = this.valorTotal.add(valor);
 }

 public UUID getFreelancerId() { return freelancerId; }
 public int getContratosConcluidos() { return contratosConcluidos; }
 public BigDecimal getValorTotal() { return valorTotal; }
}
