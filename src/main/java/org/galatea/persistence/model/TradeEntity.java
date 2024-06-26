package org.galatea.persistence.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "trade_tbl")
public class TradeEntity {

    @Id
    private long id;

    @Column(nullable = false)
    private long tradeId;

    @Column(nullable = false)
    private String account;

    @Column(nullable = false)
    private String securityId;

    @Column(nullable = false)
    private long quantity;
}