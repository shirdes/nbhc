package com.urbanairship.hbase.shc.request;

import com.google.common.collect.ImmutableList;
import com.urbanairship.hbase.shc.dispatch.ResultBroker;
import com.urbanairship.hbase.shc.response.RemoteError;
import com.urbanairship.hbase.shc.scan.ScannerBatchResult;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.ipc.RemoteException;

public final class ScannerNextBatchRequestController implements RequestController {

    private final ResultBroker<ScannerBatchResult> resultBroker;

    public ScannerNextBatchRequestController(ResultBroker<ScannerBatchResult> resultBroker) {
        this.resultBroker = resultBroker;
    }

    @Override
    public void handleResponse(HbaseObjectWritable received) {
        Object object = received.get();
        if (object == null) {
            resultBroker.communicateResult(ScannerBatchResult.allFinished());
            return;
        }

        if (!(object instanceof Result[])) {
            resultBroker.communicateError(new RuntimeException("Received result in scanner 'next' call that was not a Result[]"));
            return;
        }

        Result[] results = (Result[]) object;
        ScannerBatchResult batchResult = (results.length == 0)
                ? ScannerBatchResult.noMoreResultsInRegion()
                : ScannerBatchResult.resultsReturned(ImmutableList.copyOf(results));

        resultBroker.communicateResult(batchResult);
    }

    @Override
    public void handleRemoteError(RemoteError error, int attempt) {
        // TODO: need to understand error semantics like when a region moves.  How do we reopen a scanner
        // TODO: if needed?
        resultBroker.communicateError(new RemoteException(error.getErrorClass(),
                error.getErrorMessage().isPresent() ? error.getErrorMessage().get() : ""));
    }

    @Override
    public void handleLocalError(Throwable error, int attempt) {
        // TODO: what do we do??
    }
}
