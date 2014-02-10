package org.wizbang.hbase.nbhc.request.scan;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.Invocation;
import org.apache.hadoop.ipc.RemoteException;
import org.wizbang.hbase.nbhc.dispatch.RequestManager;
import org.wizbang.hbase.nbhc.dispatch.ResultBroker;
import org.wizbang.hbase.nbhc.request.RequestSender;
import org.wizbang.hbase.nbhc.response.RemoteError;
import org.wizbang.hbase.nbhc.response.RequestResponseController;

// TODO: this class is going to need a way to call back into the coordinator in the case that it
// TODO: receives an error from the server saying that the region we are trying to get retrieve is offline
// TODO: or not being served on the host targeted.
public final class ScannerNextBatchRequestResponseController implements RequestResponseController {

    private final HRegionLocation location;
    private final Invocation invocation;
    private final ResultBroker<ScannerBatchResult> resultBroker;
    private final RequestSender sender;
    private final RequestManager requestManager;

    private int requestId;

    public static ScannerNextBatchRequestResponseController initiate(HRegionLocation location,
                                                                     Invocation invocation,
                                                                     ResultBroker<ScannerBatchResult> resultBroker,
                                                                     RequestSender sender,
                                                                     RequestManager requestManager) {

        ScannerNextBatchRequestResponseController controller = new ScannerNextBatchRequestResponseController(
                location,
                invocation,
                resultBroker,
                sender,
                requestManager
        );

        controller.launch();

        return controller;
    }

    private ScannerNextBatchRequestResponseController(HRegionLocation location,
                                                      Invocation invocation,
                                                      ResultBroker<ScannerBatchResult> resultBroker,
                                                      RequestSender sender,
                                                      RequestManager requestManager) {
        this.location = location;
        this.invocation = invocation;
        this.resultBroker = resultBroker;
        this.sender = sender;
        this.requestManager = requestManager;
    }

    private void launch() {
        requestId = sender.sendRequest(location, invocation, this);
    }

    @Override
    public void receiveResponse(int requestId, HbaseObjectWritable received) {
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
    public void receiveRemoteError(int requestId, RemoteError remoteError) {
        // TODO: need to understand error semantics like when a region moves.  How do we reopen a scanner
        // TODO: if needed?
        resultBroker.communicateError(new RemoteException(remoteError.getErrorClass(),
                remoteError.getErrorMessage().isPresent() ? remoteError.getErrorMessage().get() : ""));
    }

    @Override
    public void receiveCommunicationError(int requestId, Throwable error) {
        // TODO: implement, this should be retriable typically?
    }

    @Override
    public void receiveFatalError(int requestId, Throwable error) {
        resultBroker.communicateError(error);
    }

    @Override
    public void cancel() {
        requestManager.unregisterResponseCallback(requestId);
    }
}
