package com.urbanairship.hbase.shc;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Hex;

import java.util.Arrays;

public final class ColumnCheck {

    private final byte[] row;
    private final byte[] family;
    private final byte[] qualifier;
    private final Optional<byte[]> value;

    public static Builder newBuilder() {
        return new Builder();
    }

    private ColumnCheck(byte[] row, byte[] family, byte[] qualifier, Optional<byte[]> value) {
        this.row = row;
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
    }

    public byte[] getRow() {
        return row;
    }

    public byte[] getFamily() {
        return family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    public Optional<byte[]> getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnCheck that = (ColumnCheck) o;

        if (!Arrays.equals(family, that.family)) return false;
        if (!Arrays.equals(qualifier, that.qualifier)) return false;
        if (!Arrays.equals(row, that.row)) return false;
        if (!value.equals(that.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(row);
        result = 31 * result + Arrays.hashCode(family);
        result = 31 * result + Arrays.hashCode(qualifier);
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("row", Hex.encodeHexString(row))
                .add("family", Hex.encodeHexString(family))
                .add("qualifier", Hex.encodeHexString(qualifier))
                .add("value", value.isPresent() ? Hex.encodeHexString(value.get()) : "[absent]")
                .toString();
    }

    public static final class Builder {

        private byte[] row = null;
        private byte[] family = null;
        private byte[] qualifier = null;
        private Optional<byte[]> value = null;

        private Builder() { }

        public Builder setRow(byte[] row) {
            this.row = row;
            return this;
        }

        public Builder setFamily(byte[] family) {
            this.family = family;
            return this;
        }

        public Builder setQualifier(byte[] qualifier) {
            this.qualifier = qualifier;
            return this;
        }

        public Builder setRequiredValue(byte[] value) {
            this.value = Optional.of(value);
            return this;
        }

        public Builder setValueNotPresent() {
            this.value = Optional.absent();
            return this;
        }

        public ColumnCheck build() {
            Preconditions.checkNotNull(row);
            Preconditions.checkNotNull(family);
            Preconditions.checkNotNull(qualifier);
            Preconditions.checkNotNull(value);

            return new ColumnCheck(row, family, qualifier, value);
        }
    }
}
