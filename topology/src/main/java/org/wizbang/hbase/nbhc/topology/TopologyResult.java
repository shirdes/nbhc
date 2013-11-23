package org.wizbang.hbase.nbhc.topology;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HRegionLocation;

import java.util.Arrays;

public final class TopologyResult {

    private final byte[] table;
    private final HRegionLocation location;

    public TopologyResult(byte[] table, HRegionLocation location) {
        this.table = Preconditions.checkNotNull(table);
        this.location = Preconditions.checkNotNull(location);
    }

    public byte[] getTable() {
        return table;
    }

    public HRegionLocation getLocation() {
        return location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopologyResult that = (TopologyResult) o;

        if (!location.equals(that.location)) return false;
        if (!Arrays.equals(table, that.table)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(table);
        result = 31 * result + location.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("table", new String(table, Charsets.UTF_8))
                .add("location", location)
                .toString();
    }
}
