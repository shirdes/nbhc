package org.wizbang.hbase.nbhc.response;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

public final class RemoteError {

    private final String errorClass;
    private final Optional<String> errorMessage;

    public RemoteError(String errorClass, Optional<String> errorMessage) {
        Preconditions.checkArgument(StringUtils.isNotBlank(errorClass));
        this.errorClass = errorClass;
        this.errorMessage = Preconditions.checkNotNull(errorMessage);
    }

    public String getErrorClass() {
        return errorClass;
    }

    public Optional<String> getErrorMessage() {
        return errorMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteError that = (RemoteError) o;

        if (!errorClass.equals(that.errorClass)) return false;
        if (!errorMessage.equals(that.errorMessage)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = errorClass.hashCode();
        result = 31 * result + errorMessage.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "RemoteError{" +
                "errorClass='" + errorClass + '\'' +
                ", errorMessage=" + errorMessage +
                '}';
    }
}
