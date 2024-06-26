package org.galatea.kafka;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.*;
import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@Entity
@Table(name = "trade_tbl")
@JsonIgnoreProperties(ignoreUnknown = true)
public class Trade implements Serializable {

    public Trade(){}

    public Trade(long tradeId, String account, String securityId, long quantity){
        this.tradeId = tradeId;
        this.account = account;
        this.securityId = securityId;
        this.quantity = quantity;
    }

    @Id
    @JsonProperty("tradeId")
    @Column(nullable = false, name = "tradeId")
    private long tradeId;

    @JsonProperty("account")
    @Column(nullable = false, name = "account")
    private String account;

    @JsonProperty("securityId")
    @Column(nullable = false, name = "securityId")
    private String securityId;

    @JsonProperty("quantity")
    @Column(nullable = false, name = "quantity")
    private long quantity;

    public long getTradeId() {
        return tradeId;
    }

    public long getQuantity() {
        return quantity;
    }

    public String getAccount() {
        return account;
    }

    public String getSecurityId() {
        return securityId;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("Trade=[tradeId=")
                .append(this.tradeId)
                .append(", account=")
                .append(this.account)
                .append(", securityId=")
                .append(this.securityId)
                .append(", quantity=")
                .append(this.quantity)
                .append("]")
                .toString();
    }
}
