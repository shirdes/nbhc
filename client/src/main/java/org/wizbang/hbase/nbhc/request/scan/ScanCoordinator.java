package org.wizbang.hbase.nbhc.request.scan;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.wizbang.hbase.nbhc.HbaseClientConfiguration;
import org.wizbang.hbase.nbhc.Protocol;
import org.wizbang.hbase.nbhc.request.RequestDetailProvider;
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
    private final RegionOwnershipTopology topology;
    private final SingleActionRequestInitiator singleActionRequestInitiator;
    private final HbaseClientConfiguration config;
    
    private final byte[] scanEndRow;

    private HRegionLocation currentLocation = null;
    private Scan currentScan;
    private long currentScannerId;

    private boolean scannerOpen = false;

    private Optional<byte[]> nextBatchRowStart = Optional.absent();

    public ScanCoordinator(String table,
                           Scan scan,
                           RegionOwnershipTopology topology,
                           SingleActionRequestInitiator singleActionRequestInitiator,
                           HbaseClientConfiguration config) {
        this.table = table;
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

        openScannerStartingAtRow(nextScannerStartKey.get());
        return true;
    }

    private void openScannerStartingAtRow(byte[] startRow) {
        currentScan.setStartRow(startRow);

        ListenableFuture<ScannerOpenResult> future = openScanner(table, currentScan);
        try {
            ScannerOpenResult result = future.get(config.openScannerTimeoutMillis, TimeUnit.MILLISECONDS);
            currentScannerId = result.getScannerId();
            currentLocation = result.getLocation();

            scannerOpen = true;
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

        ListenableFuture<ScannerBatchResult> future = getScannerNextBatch();
        try {
            ScannerBatchResult result = future.get(config.retrieveScannerBatchTimeoutMillis, TimeUnit.MILLISECONDS);
            updateNextBatchStartState(result);

            return result;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted waiting for response of retrieve next batch for scanner id " + currentScannerId, e);
        }
        catch (Exception e) {
            throw new RuntimeException("Failed retrieving next batch of results for scanner id " + currentScannerId, e);
        }
    }

    private void updateNextBatchStartState(ScannerBatchResult result) {
        if (result.getStatus() == ScannerBatchResult.Status.RESULTS_AVAILABLE) {
            byte[] next = extractStartRowQueryForNextBatch(result.getResults());
            nextBatchRowStart = Optional.of(next);
        }
        else {
            nextBatchRowStart = Optional.absent();
        }
    }

    private byte[] extractStartRowQueryForNextBatch(ImmutableList<Result> results) {
        Result last = results.get(results.size() - 1);
        byte[] lastRow = last.getRow();

        byte[] next = new byte[lastRow.length + 1];
        System.arraycopy(lastRow, 0, next, 0, lastRow.length);
        next[next.length - 1] = (byte) 0;

        return next;
    }

    private ListenableFuture<ScannerBatchResult> getScannerNextBatch() {

        RequestDetailProvider detail = new RequestDetailProvider() {
            @Override
            public HRegionLocation getLocation() {
                return currentLocation;
            }

            @Override
            public HRegionLocation getRetryLocation() {
                reopenScanner();
                return currentLocation;
            }

            @Override
            public Invocation getInvocation(HRegionLocation targetLocation) {
                return getNextBatchInvocation();
            }

            @Override
            public ImmutableSet<Class<? extends Exception>> getLocationErrors() {
                return Protocol.SCANNER_BATCH_LOCATION_ERRORS;
            }
        };

        return singleActionRequestInitiator.initiate(detail, ScannerNextBatchResponseParser.INSTANCE);
    }

    private Invocation getNextBatchInvocation() {
        return new Invocation(SCANNER_NEXT_TARGET_METHOD, TARGET_PROTOCOL, new Object[]{
                currentScannerId,
                config.scannerBatchSize
        });
    }

    private void reopenScanner() {
        // TODO: what about the situation where, we open the scanner, and then on the first request for a batch of
        // TODO: results, we get a retry situation? In that case, we will not have set nextBatchRowStart yet, and
        // TODO: this would fail. Thinking we should be setting the nextBatchRowStart on a scanner open?
        if (!nextBatchRowStart.isPresent()) {
            throw new RuntimeException("Scan sequence not in a state with a known next batch start row. Reset of scanner should not have been attempted!");
        }

        closeOpenScanner();
        openScannerStartingAtRow(nextBatchRowStart.get());
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

            @Override
            public ImmutableSet<Class<? extends Exception>> getLocationErrors() {
                return Protocol.STANDARD_LOCATION_ERRORS;
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
