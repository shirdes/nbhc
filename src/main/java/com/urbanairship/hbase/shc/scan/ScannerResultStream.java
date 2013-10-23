package com.urbanairship.hbase.shc.scan;

import com.google.common.collect.AbstractIterator;
import com.urbanairship.hbase.shc.RegionOwnershipTopology;
import org.apache.hadoop.hbase.client.Result;
import sun.org.mozilla.javascript.internal.Function;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class ScannerResultStream extends AbstractIterator<Result> implements Closeable {

    private SingleRegionScannerResultStream currentRegionStream;

    @Override
    protected Result computeNext() {
        if (currentStreamHasNext()) {
            return currentRegionStream.next();
        }


        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private boolean currentStreamHasNext() {
        return currentRegionStream != null && currentRegionStream.hasNext();
    }

    @Override
    public void close() throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
