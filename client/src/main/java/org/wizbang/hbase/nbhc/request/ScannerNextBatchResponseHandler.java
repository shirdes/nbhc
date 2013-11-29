package org.wizbang.hbase.nbhc.request;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.ipc.RemoteException;
import org.wizbang.hbase.nbhc.dispatch.ResultBroker;
import org.wizbang.hbase.nbhc.response.RemoteError;
import org.wizbang.hbase.nbhc.scan.ScannerBatchResult;

public final class ScannerNextBatchResponseHandler implements ResponseHandler {

    private final ResultBroker<ScannerBatchResult> resultBroker;

    public ScannerNextBatchResponseHandler(ResultBroker<ScannerBatchResult> resultBroker) {
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
        resultBroker.communicateError(error);
    }
}
