package org.apache.flink.training.assignments.domain;

import java.math.BigDecimal;
import java.util.Objects;

public class Position extends IncomingEvent {
    private static final long serialVersionUID = -8898987593987711L;
    private String cusip;
    private BuySell buySell;
    private String account;
    private String subAccount;
    private int quantity;
    private String orderId;
    private BigDecimal price;
    private BigDecimal marketValue;
    private String positionKey;

    public String getPositionKey() {
        return positionKey;
    }

    public void setPositionKey(String positionKey) {
        this.positionKey = positionKey;
    }

    public Position(String cusip, BuySell buySell, String account, String subAccount, int quantity, String orderId) {
        this.account = account;
        this.subAccount = subAccount;
        this.quantity = quantity;
        this.cusip = cusip;
        this.orderId = orderId;
        this.buySell = buySell;
        this.price = price;
        this.marketValue = marketValue;
    }

    public Position(String cusip, int quantity, String orderId) {
        this.quantity = quantity;
        this.cusip = cusip;
        this.orderId = orderId;
    }
    public Position() {
    }

    @Override
    public String toString() {
        return "Position{" +
                "cusip='" + cusip + '\'' +
                ", account='" + account + '\'' +
                ", subAccount='" + subAccount + '\'' +
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
        Position that = (Position) o;
        return quantity == that.quantity &&
                Objects.equals(cusip, that.cusip) &&
                buySell == that.buySell &&
                Objects.equals(account, that.account) &&
                Objects.equals(subAccount, that.subAccount) &&
                Objects.equals(price, that.price) &&
                Objects.equals(marketValue, that.marketValue);
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

    @Override
    public int hashCode() {
        return Objects.hash(cusip, buySell, account, subAccount, quantity,price,marketValue);
    }

    public void setCusip(String cusip) {
        this.cusip = cusip;
    }

    public void setBuySell(BuySell buySell) {
        this.buySell = buySell;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public void setSubAccount(String subAccount) {
        this.subAccount = subAccount;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String getCusip() {
        return cusip;
    }

    public BuySell getBuySell() {
        return buySell;
    }

    public String getAccount() {
        return account;
    }

    public String getSubAccount() {
        return subAccount;
    }

    public int getQuantity() {
        return quantity;
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
