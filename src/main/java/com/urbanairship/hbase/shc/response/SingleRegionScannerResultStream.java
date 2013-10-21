package com.urbanairship.hbase.shc.response;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.Queue;

public class SingleRegionScannerResultStream extends AbstractIterator<Result> implements ScannerResultStream {

    private final HRegionLocation location;
    private final Scan scan;
    private final Function<HRegionLocation, Long> scannerOpener;
    private final Supplier<ScannerBatchResult> batchLoader;

    private boolean open;
    private long currentScannerId;

    private Queue<Result> currentResults;

    public SingleRegionScannerResultStream(HRegionLocation location, Scan scan, Function<HRegionLocation, Long> scannerOpener, Supplier<ScannerBatchResult> batchLoader) {
        this.location = location;
        this.scan = scan;
        this.scannerOpener = scannerOpener;
        this.batchLoader = batchLoader;

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

        ScannerBatchResult nextBatch = batchLoader.get();
        if (nextBatch.getStatus() == ScannerBatchResult.Status.NO_MORE_RESULTS_IN_REGION ||
                nextBatch.getStatus() == ScannerBatchResult.Status.FINISHED) {
            return endOfData();
        }

        return null;
    }

    @Override
    public void close() throws IOException {

        //To change body of implemented methods use File | Settings | File Templates.
    }
}
