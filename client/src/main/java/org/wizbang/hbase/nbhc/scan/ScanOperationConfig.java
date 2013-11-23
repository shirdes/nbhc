package org.wizbang.hbase.nbhc.scan;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public final class ScanOperationConfig {

    private final long openScannerTimeoutMillis;
    private final long retrieveScannerBatchTimeoutMillis;

    public static Builder newBuilder() {
        return new Builder();
    }

    private ScanOperationConfig(long openScannerTimeoutMillis,
                                long retrieveScannerBatchTimeoutMillis) {
        this.openScannerTimeoutMillis = openScannerTimeoutMillis;
        this.retrieveScannerBatchTimeoutMillis = retrieveScannerBatchTimeoutMillis;
    }

    public long getOpenScannerTimeoutMillis() {
        return openScannerTimeoutMillis;
    }

    public long getRetrieveScannerBatchTimeoutMillis() {
        return retrieveScannerBatchTimeoutMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScanOperationConfig that = (ScanOperationConfig) o;

        if (openScannerTimeoutMillis != that.openScannerTimeoutMillis) return false;
        if (retrieveScannerBatchTimeoutMillis != that.retrieveScannerBatchTimeoutMillis) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (openScannerTimeoutMillis ^ (openScannerTimeoutMillis >>> 32));
        result = 31 * result + (int) (retrieveScannerBatchTimeoutMillis ^ (retrieveScannerBatchTimeoutMillis >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("openScannerTimeoutMillis", openScannerTimeoutMillis)
                .add("retrieveScannerBatchTimeoutMillis", retrieveScannerBatchTimeoutMillis)
                .toString();
    }

    public static final class Builder {

        private long openScannerTimeoutMillis = 30000L;
        private long retrieveScannerBatchTimeoutMillis = 30000L;

        private Builder() { }

        public Builder setOpenScannerTimeoutMillis(long openScannerTimeoutMillis) {
            this.openScannerTimeoutMillis = openScannerTimeoutMillis;
            return this;
        }

        public Builder setRetrieveScannerBatchTimeoutMillis(long retrieveScannerBatchTimeoutMillis) {
            this.retrieveScannerBatchTimeoutMillis = retrieveScannerBatchTimeoutMillis;
            return this;
        }

        public ScanOperationConfig build() {
            Preconditions.checkArgument(openScannerTimeoutMillis > 0L);
            Preconditions.checkArgument(retrieveScannerBatchTimeoutMillis > 0L);

            return new ScanOperationConfig(openScannerTimeoutMillis, retrieveScannerBatchTimeoutMillis);
        }
    }
}
