package org.wizbang.hbase.nbhc.scan;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Result;

import java.util.Queue;

public class SingleRegionScannerResultStream extends AbstractIterator<Result> {

    private final Supplier<ScannerBatchResult> batchLoader;

    private Queue<Result> currentResults;

    private ScannerBatchResult.Status finalStatus;

    public SingleRegionScannerResultStream(Supplier<ScannerBatchResult> batchLoader) {
        this.batchLoader = batchLoader;

        this.currentResults = Lists.newLinkedList();
    }

    @Override
    protected Result computeNext() {
        if (!currentResults.isEmpty()) {
            return currentResults.remove();
        }

        ScannerBatchResult nextBatch = batchLoader.get();
        if (nextBatch.getStatus() != ScannerBatchResult.Status.RESULTS_AVAILABLE) {
            finalStatus = nextBatch.getStatus();
            return endOfData();
        }

        Preconditions.checkArgument(nextBatch.getResults().size() > 0);

        currentResults = Lists.newLinkedList(nextBatch.getResults());
        return currentResults.remove();
    }

    public ScannerBatchResult.Status getFinalStatus() {
        Preconditions.checkState(finalStatus != null);
        return finalStatus;
    }
}
