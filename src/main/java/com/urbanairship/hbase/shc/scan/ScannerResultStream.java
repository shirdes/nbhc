package com.urbanairship.hbase.shc.scan;

import org.apache.hadoop.hbase.client.Result;

import java.io.Closeable;
import java.util.Iterator;

public interface ScannerResultStream extends Iterator<Result>, Closeable {
}
