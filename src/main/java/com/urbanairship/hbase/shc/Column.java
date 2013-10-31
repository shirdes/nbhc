package com.urbanairship.hbase.shc;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Hex;

import java.util.Arrays;

public final class Column {

    private final byte[] row;
    private final byte[] family;
    private final byte[] qualifier;

    public static Builder newBuilder() {
        return new Builder();
    }

    private Column(byte[] row, byte[] family, byte[] qualifier) {
        this.row = row;
        this.family = family;
        this.qualifier = qualifier;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Column column = (Column) o;

        if (!Arrays.equals(family, column.family)) {
            return false;
        }
        if (!Arrays.equals(qualifier, column.qualifier)) {
            return false;
        }
        if (!Arrays.equals(row, column.row)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(row);
        result = 31 * result + Arrays.hashCode(family);
        result = 31 * result + Arrays.hashCode(qualifier);
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("row", Hex.encodeHexString(row))
                .add("family", Hex.encodeHex(family))
                .add("qualifier", Hex.encodeHexString(qualifier))
                .toString();
    }

    public static final class Builder {

        private byte[] row = null;
        private byte[] family = null;
        private byte[] qualifier = null;

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

        public Column build() {
            Preconditions.checkNotNull(row);
            Preconditions.checkNotNull(family);
            Preconditions.checkNotNull(qualifier);

            return new Column(row, family, qualifier);
        }
    }
}
