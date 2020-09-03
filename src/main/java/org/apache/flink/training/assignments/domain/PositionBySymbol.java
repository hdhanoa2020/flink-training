package org.apache.flink.training.assignments.domain;

import java.math.BigDecimal;
import java.util.Objects;

public class PositionBySymbol extends IncomingEvent {
    private static final long serialVersionUID = -889985658676665651L;
    private String cusip;
    private int quantity;
    private String orderId;
    private BigDecimal price;
    private BigDecimal marketValue;
    private BuySell buySell;
    private String account;
    private String subAccount;
    private String positionKey;

    public String getPositionKey() {
        return positionKey;
    }

    public void setPositionKey(String positionKey) {
        this.positionKey = positionKey;
    }

    public BuySell getBuySell() {
        return buySell;
    }

    public void setBuySell(BuySell buySell) {
        this.buySell = buySell;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getSubAccount() {
        return subAccount;
   }

    public void setSubAccount(String subAccount) {
        this.subAccount = subAccount;
    }

    public PositionBySymbol(String cusip, int quantity, String orderId) {
        this.quantity = quantity;
        this.cusip = cusip;
        this.price = price;
        this.marketValue = marketValue;
        this.orderId = orderId;
    }

    public PositionBySymbol() {
    }


    @Override
    public String toString() {
        return "PositionBySymbol{" +
                "cusip='" + cusip + '\'' +
                ", quantity=" + quantity +
                ", price=" + price +
                ", marketValue=" + marketValue +
                ", positionKey=" + positionKey +
                ", orderId=" + orderId +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PositionBySymbol that = (PositionBySymbol) o;
        return quantity == that.quantity &&
                Objects.equals(cusip, that.cusip) &&
                Objects.equals(price, that.price) &&
                Objects.equals(marketValue, that.marketValue);

    }

    @Override
    public int hashCode() {
        return Objects.hash(cusip, quantity);
    }

    public void setCusip(String cusip) {
        this.cusip = cusip;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String getCusip() {
        return cusip;
    }

    public int getQuantity() {
        return quantity;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public BigDecimal getMarketValue() {
        return marketValue;
    }

    public void setMarketValue(BigDecimal marketValue) {
        this.marketValue = marketValue;
    }
   
    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getOrderId() {
        return orderId;
    }

    @Override
    public byte[] key() {
        return new byte[0];
    }
}
