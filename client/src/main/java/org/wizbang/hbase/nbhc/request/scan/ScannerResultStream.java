package org.wizbang.hbase.nbhc.request.scan;

import com.google.common.base.Supplier;
import com.google.common.collect.AbstractIterator;
import org.apache.hadoop.hbase.client.Result;

import java.io.Closeable;
import java.io.IOException;

public final class ScannerResultStream extends AbstractIterator<Result> implements Closeable {

    private final ScanCoordinator scanCoordinator;

    private final Supplier<ScannerBatchResult> batchLoader;

    private SingleRegionScannerResultStream currentRegionStream = null;

    public ScannerResultStream(ScanCoordinator scanCoordinator) {
        this.scanCoordinator = scanCoordinator;
        this.batchLoader = new Supplier<ScannerBatchResult>() {
            @Override
            public ScannerBatchResult get() {
                return ScannerResultStream.this.scanCoordinator.loadNextBatch();
            }
        };
    }

    @Override
    protected Result computeNext() {
        while (!isNextAvailableFromCurrentStream() && isNextRegionStreamAvailable()) {
            if (!scanCoordinator.openNextScannerId()) {
                break;
            }

            currentRegionStream = new SingleRegionScannerResultStream(batchLoader);
        }

        return isNextAvailableFromCurrentStream() ? currentRegionStream.next() : endOfData();
    }

    private boolean isNextRegionStreamAvailable() {
        return currentRegionStream == null ||
                currentRegionStream.getFinalStatus() == ScannerBatchResult.Status.NO_MORE_RESULTS_IN_REGION;
    }

    private boolean isNextAvailableFromCurrentStream() {
        return currentRegionStream != null && currentRegionStream.hasNext();
    }

    @Override
    public void close() throws IOException {
        scanCoordinator.closeOpenScanner();
    }
}
