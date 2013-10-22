package com.urbanairship.hbase.shc.scan;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Result;

import java.io.Closeable;
import java.io.IOException;
import java.util.Queue;

public class SingleRegionScannerResultStream extends AbstractIterator<Result> implements ScannerResultStream {

    private final HRegionLocation location;
    private final Function<HRegionLocation, Long> scannerOpener;
    private final Function<Long, ScannerBatchResult> batchLoader;
    private final Closeable closeFunction;

    private boolean open;
    private long currentScannerId;

    private Queue<Result> currentResults;

    private ScannerBatchResult.Status finalStatus;

    public SingleRegionScannerResultStream(HRegionLocation location,
                                           Function<HRegionLocation, Long> scannerOpener,
                                           Function<Long, ScannerBatchResult> batchLoader,
                                           Closeable closeFunction) {
        this.location = location;
        this.scannerOpener = scannerOpener;
        this.batchLoader = batchLoader;
        this.closeFunction = closeFunction;

        this.open = false;
        this.currentResults = Lists.newLinkedList();
    }

    @Override
    protected Result computeNext() {
        if (!open) {
            currentScannerId = scannerOpener.apply(location);
        }

        if (!currentResults.isEmpty()) {
            return currentResults.remove();
        }

        // TODO: how do we handle the possibility that we might need to open a new scanner?
        ScannerBatchResult nextBatch = batchLoader.apply(currentScannerId);
        if (nextBatch.getStatus() != ScannerBatchResult.Status.RESULTS_AVAILABLE) {
            finalStatus = nextBatch.getStatus();
            return endOfData();
        }

        Preconditions.checkArgument(nextBatch.getResults().size() > 0);

        currentResults = Lists.newLinkedList(nextBatch.getResults());
        return currentResults.remove();
    }

    @Override
    public void close() throws IOException {
        closeFunction.close();
    }

    public ScannerBatchResult.Status getFinalStatus() {
        Preconditions.checkState(finalStatus != null);
        return finalStatus;
    }
}
