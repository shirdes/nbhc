package com.urbanairship.hbase.shc.scan;

import org.apache.hadoop.hbase.client.Scan;

public interface ScanOpener {

    long openScanner(String table, Scan scan);

}
