package com.urbanairship.hbase.shc.operation;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Method;
import java.util.Arrays;

public final class RpcRequestDetail<P> {

    private final String table;
    private final byte[] targetRow;
    private final Method targetMethod;
    private final P param;

    public static <P> Builder<P> newBuilder() {
        return new Builder<P>();
    }

    private RpcRequestDetail(String table,
                             byte[] targetRow,
                             Method targetMethod,
                             P param) {
        this.table = table;
        this.targetRow = targetRow;
        this.targetMethod = targetMethod;
        this.param = param;
    }

    public String getTable() {
        return table;
    }

    public byte[] getTargetRow() {
        return targetRow;
    }

    public Method getTargetMethod() {
        return targetMethod;
    }

    public P getParam() {
        return param;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RpcRequestDetail that = (RpcRequestDetail) o;

        if (!param.equals(that.param)) {
            return false;
        }
        if (!table.equals(that.table)) {
            return false;
        }
        if (!targetMethod.equals(that.targetMethod)) {
            return false;
        }
        if (!Arrays.equals(targetRow, that.targetRow)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = table.hashCode();
        result = 31 * result + Arrays.hashCode(targetRow);
        result = 31 * result + targetMethod.hashCode();
        result = 31 * result + param.hashCode();
        return result;
    }

    // TODO: toString()?

    public static final class Builder<P> {

        private String table = null;
        private byte[] targetRow = null;
        private Method targetMethod = null;
        private P param = null;

        private Builder() { }

        public Builder<P> setTable(String table) {
            this.table = table;
            return this;
        }

        public Builder<P> setTargetRow(byte[] targetRow) {
            this.targetRow = targetRow;
            return this;
        }

        public Builder<P> setTargetMethod(Method targetMethod) {
            this.targetMethod = targetMethod;
            return this;
        }

        public Builder<P> setParam(P param) {
            this.param = param;
            return this;
        }

        public RpcRequestDetail<P> build() {
            Preconditions.checkArgument(StringUtils.isNotBlank(table));
            Preconditions.checkNotNull(targetRow);
            Preconditions.checkNotNull(targetMethod);
            Preconditions.checkNotNull(param);

            return new RpcRequestDetail<P>(table, targetRow, targetMethod, param);
        }
    }
}
