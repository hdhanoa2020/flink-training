package org.apache.flink.training.assignments.domain;

import java.util.List;
import java.util.Objects;


public class ComplianceResult extends IncomingEvent implements KeyedAware {

    private static final long serialVersionUID = -296784558714885137L;

    private String orderId;
    private boolean success;

    private List<String> errors;

    public ComplianceResult() {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ComplianceResult)) return false;
        ComplianceResult that = (ComplianceResult) o;
        return isSuccess() == that.isSuccess() &&
                Objects.equals(getOrderId(), that.getOrderId()) &&
                Objects.equals(getErrors(), that.getErrors());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getOrderId(), isSuccess(), getErrors());
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public List<String> getErrors() {
        return errors;
    }

    public void setErrors(List<String> errors) {
        this.errors = errors;
    }

    @Override
    public byte[] key() {
        return orderId.getBytes();
    }

}