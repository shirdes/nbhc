package org.wizbang.hbase.nbhc.request.multi;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hbase.client.Result;

public final class MultiActionResponse {

    private final boolean errorResponse;
    private final Throwable error;
    private final ImmutableMap<Integer, Result> results;
    private final ImmutableSet<Integer> indexesNeedingRetry;

    public static MultiActionResponse error(Throwable error) {
        return new MultiActionResponse(true, Preconditions.checkNotNull(error), null, null);
    }

    public static MultiActionResponse result(ImmutableMap<Integer, Result> results,
                                             ImmutableSet<Integer> indexesNeedingRetry) {
        return new MultiActionResponse(false, null, Preconditions.checkNotNull(results),
                Preconditions.checkNotNull(indexesNeedingRetry));
    }

    private MultiActionResponse(boolean errorResponse,
                                Throwable error,
                                ImmutableMap<Integer, Result> results,
                                ImmutableSet<Integer> indexesNeedingRetry) {
        this.errorResponse = errorResponse;
        this.error = error;
        this.results = results;
        this.indexesNeedingRetry = indexesNeedingRetry;
    }

    public boolean isErrorResponse() {
        return errorResponse;
    }

    public Throwable getError() {
        Preconditions.checkState(errorResponse);
        return error;
    }

    public ImmutableMap<Integer, Result> getResults() {
        Preconditions.checkState(!errorResponse);
        return results;
    }

    public ImmutableSet<Integer> getIndexesNeedingRetry() {
        Preconditions.checkState(!errorResponse);
        return indexesNeedingRetry;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MultiActionResponse that = (MultiActionResponse) o;

        if (errorResponse != that.errorResponse) return false;
        if (!error.equals(that.error)) return false;
        if (!indexesNeedingRetry.equals(that.indexesNeedingRetry)) return false;
        if (!results.equals(that.results)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (errorResponse ? 1 : 0);
        result = 31 * result + error.hashCode();
        result = 31 * result + results.hashCode();
        result = 31 * result + indexesNeedingRetry.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("errorResponse", errorResponse)
                .add("error", error)
                .add("results", results)
                .add("indexesNeedingRetry", indexesNeedingRetry)
                .toString();
    }
}
