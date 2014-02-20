package org.wizbang.hbase.nbhc.request.scan;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public final class ScannerNextBatchResponseParser implements Function<HbaseObjectWritable, ScannerBatchResult> {

    public static final ScannerNextBatchResponseParser INSTANCE = new ScannerNextBatchResponseParser();

    private ScannerNextBatchResponseParser() { }

    @Override
    public ScannerBatchResult apply(HbaseObjectWritable received) {
        Object object = received.get();
        if (object == null) {
            return ScannerBatchResult.allFinished();
        }

        if (!(object instanceof Result[])) {
            throw new RuntimeException("Received result in scanner 'next' call that was not a Result[]");
        }

        Result[] results = (Result[]) object;
        return (results.length == 0)
                ? ScannerBatchResult.noMoreResultsInRegion()
                : ScannerBatchResult.resultsReturned(ImmutableList.copyOf(results));
    }
}
