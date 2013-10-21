package com.urbanairship.hbase.shc.response;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.client.Result;

public final class ScannerBatchResult {

    public enum Status {
        RESULTS_RETURNED, NO_MORE_RESULTS_IN_REGION, FINISHED
    }

    private static final ScannerBatchResult NO_MORE_RESULTS_IN_REGION_RESULT =
            new ScannerBatchResult(Status.NO_MORE_RESULTS_IN_REGION, ImmutableList.<Result>of());

    private static final ScannerBatchResult FINISHED_RESULT =
            new ScannerBatchResult(Status.FINISHED, ImmutableList.<Result>of());

    public static ScannerBatchResult resultsReturned(ImmutableList results) {
        return new ScannerBatchResult(Status.RESULTS_RETURNED, results);
    }

    public static ScannerBatchResult noMoreResultsInRegion() {
        return NO_MORE_RESULTS_IN_REGION_RESULT;
    }

    public static ScannerBatchResult finished() {
        return FINISHED_RESULT;
    }

    private final Status status;
    private final ImmutableList<Result> results;

    private ScannerBatchResult(Status status, ImmutableList<Result> results) {
        this.status = status;
        this.results = results;
    }

    public Status getStatus() {
        return status;
    }

    public ImmutableList<Result> getResults() {
        Preconditions.checkState(status == Status.RESULTS_RETURNED);
        return results;
    }

}
