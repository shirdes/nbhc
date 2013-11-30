package org.wizbang.hbase.nbhc.request.scan;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public final class ScanController {

    private static final Logger log = LogManager.getLogger(ScanController.class);

    private final String table;
    private final ScanOpener opener;
    private final ScanResultsLoader resultsLoader;
    private final ScanCloser closer;
    private final ScanOperationConfig config;
    
    private final byte[] scanEndRow;

    private HRegionLocation currentLocation = null;
    private Scan currentScan;
    private long currentScannerId;

    private boolean scannerOpen = false;

    public ScanController(String table,
                          Scan scan,
                          ScanOpener opener,
                          ScanResultsLoader resultsLoader,
                          ScanCloser closer,
                          ScanOperationConfig config) {
        this.table = table;
        this.opener = opener;
        this.resultsLoader = resultsLoader;
        this.closer = closer;
        this.config = config;

        this.currentScan = scan;
        this.scanEndRow = scan.getStopRow() != null ? scan.getStopRow() : HConstants.EMPTY_END_ROW;
    }

    public boolean openNextScannerId() {
        closeOpenScanner();

        Optional<byte[]> nextScannerStartKey = getNextScannerStartKey();
        if (!nextScannerStartKey.isPresent()) {
            return false;
        }

        currentScan.setStartRow(nextScannerStartKey.get());

        ListenableFuture<ScannerOpenResult> future = opener.open(table, currentScan);
        try {
            ScannerOpenResult result = future.get(config.getOpenScannerTimeoutMillis(), TimeUnit.MILLISECONDS);
            currentScannerId = result.getScannerId();
            currentLocation = result.getLocation();

            scannerOpen = true;
            return true;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for response of open scanner call", e);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to open scanner", e);
        }
    }

    public ScannerBatchResult loadNextBatch() {
        Preconditions.checkState(scannerOpen);

        ListenableFuture<ScannerBatchResult> future = resultsLoader.load(currentLocation, currentScannerId);
        try {
            return future.get(config.getRetrieveScannerBatchTimeoutMillis(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for response of retrieve next batch for scanner id " + currentScannerId, e);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed retrieving next batch of results for scanner id " + currentScannerId, e);
        }
    }

    public void closeOpenScanner() {
        if (!scannerOpen) {
            return;
        }

        scannerOpen = false;
        final long closingScannerId = currentScannerId;
        ListenableFuture<Void> future = closer.close(currentLocation, currentScannerId);
        Futures.addCallback(future, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void result) {
                log.debug("Successfully closed scanner id " + closingScannerId);
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Failed to close scanner id " + closingScannerId, t);
            }
        });
    }

    public Optional<byte[]> getNextScannerStartKey() {
        if (currentLocation == null) {
            return Optional.of(currentScan.getStartRow() != null ? currentScan.getStartRow() : HConstants.EMPTY_START_ROW);
        } 
        
        byte[] endKey = currentLocation.getRegionInfo().getEndKey();
        if (isScanComplete(endKey)) {
            return Optional.absent();
        }
        
        return Optional.of(endKey);
    }

    private boolean isScanComplete(byte[] currentRegionEndKey) {
        return currentRegionEndKey == null ||
                Arrays.equals(currentRegionEndKey, HConstants.EMPTY_BYTE_ARRAY) ||
                isBeyondScanEnd(currentRegionEndKey);
    }

    private boolean isBeyondScanEnd(byte[] key) {
        return scanEndRow.length > 0 && Bytes.compareTo(scanEndRow, key) <= 0;

    }
}
