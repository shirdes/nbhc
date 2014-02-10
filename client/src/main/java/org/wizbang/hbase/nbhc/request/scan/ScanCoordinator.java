package org.wizbang.hbase.nbhc.request.scan;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.wizbang.hbase.nbhc.HbaseClientConfiguration;
import org.wizbang.hbase.nbhc.RetryExecutor;
import org.wizbang.hbase.nbhc.dispatch.HbaseOperationResultFuture;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.request.RequestDetailProvider;
import org.wizbang.hbase.nbhc.request.RequestSender;
import org.wizbang.hbase.nbhc.request.SingleActionRequestInitiator;
import org.wizbang.hbase.nbhc.topology.RegionOwnershipTopology;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.wizbang.hbase.nbhc.Protocol.*;

// TODO: right now we are taking the scan batch size from a configuration parameter rather than the cacheSize value on
// TODO: the Scan object.  Need to think through that more...
public final class ScanCoordinator {

    private static final Logger log = LogManager.getLogger(ScanCoordinator.class);

    private final String table;
    private final RequestSender sender;
    private final RequestManager requestManager;
    private final RetryExecutor retryExecutor;
    private final RegionOwnershipTopology topology;
    private final SingleActionRequestInitiator singleActionRequestInitiator;
    private final HbaseClientConfiguration config;
    
    private final byte[] scanEndRow;

    private HRegionLocation currentLocation = null;
    private Scan currentScan;
    private long currentScannerId;

    private boolean scannerOpen = false;

    public ScanCoordinator(String table,
                           Scan scan,
                           RequestSender sender,
                           RequestManager requestManager,
                           RetryExecutor retryExecutor,
                           RegionOwnershipTopology topology,
                           SingleActionRequestInitiator singleActionRequestInitiator,
                           HbaseClientConfiguration config) {
        this.table = table;
        this.sender = sender;
        this.requestManager = requestManager;
        this.retryExecutor = retryExecutor;
        this.topology = topology;
        this.singleActionRequestInitiator = singleActionRequestInitiator;
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

        ListenableFuture<ScannerOpenResult> future = openScanner(table, currentScan);
        try {
            ScannerOpenResult result = future.get(config.openScannerTimeoutMillis, TimeUnit.MILLISECONDS);
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

    private ListenableFuture<ScannerOpenResult> openScanner(final String table, final Scan scan) {
        final OpenScannerRequestDetailProvider locationCapturingProvider =
                new OpenScannerRequestDetailProvider(table, scan, topology);

        ListenableFuture<Long> future = singleActionRequestInitiator.initiate(locationCapturingProvider,
                OPEN_SCANNER_RESPONSE_PARSER);

        return Futures.transform(future, new Function<Long, ScannerOpenResult>() {
            @Override
            public ScannerOpenResult apply(Long scannerId) {
                return new ScannerOpenResult(locationCapturingProvider.getLastRequestLocation(), scannerId);
            }
        });
    }

    public ScannerBatchResult loadNextBatch() {
        Preconditions.checkState(scannerOpen);

        ListenableFuture<ScannerBatchResult> future = getScannerNextBatch(currentLocation, currentScannerId, config.scannerBatchSize);
        try {
            return future.get(config.retrieveScannerBatchTimeoutMillis, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for response of retrieve next batch for scanner id " + currentScannerId, e);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed retrieving next batch of results for scanner id " + currentScannerId, e);
        }
    }

    private ListenableFuture<ScannerBatchResult> getScannerNextBatch(final HRegionLocation location,
                                                                     long scannerId,
                                                                     int numResults) {

        Invocation invocation = new Invocation(SCANNER_NEXT_TARGET_METHOD, TARGET_PROTOCOL, new Object[]{
                scannerId,
                numResults
        });

        HbaseOperationResultFuture<ScannerBatchResult> future = new HbaseOperationResultFuture<ScannerBatchResult>();

        // TODO: what happens if the server says this region is not online or something and we should go back and find
        // TODO: the updated region to issue the request to?  Somehow that will need to bubble all the way out to the
        // TODO: state holder but don't want to tie this directly into that either :(
        final ScannerNextBatchRequestResponseController controller = ScannerNextBatchRequestResponseController.initiate(
                location, invocation, future, sender, requestManager);

        future.setCancelCallback(new Runnable() {
            @Override
            public void run() {
                controller.cancel();
            }
        });

        return future;
    }

    public void closeOpenScanner() {
        if (!scannerOpen) {
            return;
        }

        scannerOpen = false;
        final long closingScannerId = currentScannerId;
        ListenableFuture<Void> future = closeScanner(currentLocation, currentScannerId);
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

    private ListenableFuture<Void> closeScanner(final HRegionLocation location, long scannerId) {
        final Invocation invocation = new Invocation(CLOSE_SCANNER_TARGET_METHOD, TARGET_PROTOCOL, new Object[]{scannerId});
        RequestDetailProvider detailProvider = new RequestDetailProvider() {
            @Override
            public HRegionLocation getLocation() {
                return location;
            }

            @Override
            public HRegionLocation getRetryLocation() {
                return location;
            }

            @Override
            public Invocation getInvocation(HRegionLocation targetLocation) {
                return invocation;
            }
        };

        return singleActionRequestInitiator.initiate(detailProvider, CLOSE_SCANNER_RESPONSE_PARSER);
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
