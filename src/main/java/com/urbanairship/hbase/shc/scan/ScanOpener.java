package com.urbanairship.hbase.shc.scan;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.client.Scan;

public interface ScanOpener {

    ListenableFuture<ScannerOpenResult> open(String table, Scan scan);

}
