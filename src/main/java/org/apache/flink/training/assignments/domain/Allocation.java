package org.apache.flink.training.assignments.domain;

import java.io.Serializable;

import java.util.Objects;

public class Allocation implements Serializable {
    private static final long serialVersionUID = -887981985593847911L;
    private String account;
    private String subAccount;
    private int quantity;

    private int orderSize;
    private String orderId;

    public Allocation(String account, String subAccount, int quantity) {
        this.account = account;
        this.subAccount = subAccount;
        this.quantity = quantity;
        this.orderSize = orderSize;
        this.orderId = orderId;
    }

    public Allocation() {
    }



    public String getAccount() {
        return this.account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getSubAccount() {
        return this.subAccount;
    }

    public void setSubAccount(String subAccount) {
        this.subAccount = subAccount;
    }

    public int getQuantity() {
        return this.quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public void setOrderSize(int orderSize) {
        this.orderSize = orderSize;
    }
    public int getOrderSize() {
        return this.orderSize;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }
    public String getOrderId() {
        return this.orderId;
    }
    public String toString() {
        String var10000 = this.getAccount();
        return "Allocation(account=" + var10000 + ", subAccount=" + this.getSubAccount() + ", quantity=" + this.getQuantity() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Allocation that = (Allocation) o;
        return quantity == that.quantity &&
                Objects.equals(account, that.account) &&
                Objects.equals(subAccount, that.subAccount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(account, subAccount, quantity,orderSize,orderId);

    }

    protected boolean canEqual(Object other) {
        return other instanceof Allocation;
    }

}
