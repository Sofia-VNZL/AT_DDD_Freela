package br.com.freela.reputacao;


import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table (name= "eventos_processados")
class EventoProcessado {
    @Id
    UUID eventId;

    @Column(nullable = false, length = 80)
    String eventType;

    @Column(nullable = false)
    UUID contratoId;

    @Column(nullable = false)
    Instant processadoEm;

    protected EventoProcessado() {}

    EventoProcessado(UUID eventId, String eventType, UUID contratoId) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.contratoId = contratoId;
        this.processadoEm = Instant.now();
    }
}