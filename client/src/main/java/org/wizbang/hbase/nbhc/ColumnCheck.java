package org.wizbang.hbase.nbhc;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Hex;

public final class ColumnCheck {

    private final Column column;
    private final Optional<byte[]> value;

    public static Builder newBuilder() {
        return new Builder();
    }

    public ColumnCheck(Column column, Optional<byte[]> value) {
        this.column = column;
        this.value = value;
    }

    public Column getColumn() {
        return column;
    }

    public Optional<byte[]> getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ColumnCheck that = (ColumnCheck) o;

        if (!column.equals(that.column)) {
            return false;
        }
        if (!value.equals(that.value)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = column.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("column", column)
                .add("value", value.isPresent() ? Hex.encodeHexString(value.get()) : "[absent]")
                .toString();
    }

    public static final class Builder {

        private final Column.Builder column = Column.newBuilder();

        private Optional<byte[]> value = null;

        private Builder() { }

        public Builder setRow(byte[] row) {
            column.setRow(row);
            return this;
        }

        public Builder setFamily(byte[] family) {
            column.setFamily(family);
            return this;
        }

        public Builder setQualifier(byte[] qualifier) {
            column.setQualifier(qualifier);
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
            Preconditions.checkNotNull(value);

            return new ColumnCheck(column.build(), value);
        }
    }
}
