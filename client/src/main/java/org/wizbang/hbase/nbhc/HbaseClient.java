package org.wizbang.hbase.nbhc;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.wizbang.hbase.nbhc.request.scan.ScannerResultStream;

public interface HbaseClient {
    ListenableFuture<Result> get(String table, Get get);

    ListenableFuture<ImmutableList<Result>> multiGet(String table, ImmutableList<Get> gets);

    ListenableFuture<Void> put(String table, Put put);

    ListenableFuture<Void> multiPut(String table, ImmutableList<Put> puts);

    ListenableFuture<Boolean> checkAndPut(String table, ColumnCheck check, Put put);

    ListenableFuture<Void> delete(String table, Delete delete);

    ListenableFuture<Void> multiDelete(String table, ImmutableList<Delete> deletes);

    ListenableFuture<Boolean> checkAndDelete(String table, ColumnCheck check, Delete delete);

    ScannerResultStream getScannerStream(String table, Scan scan);

    ListenableFuture<Long> incrementColumnValue(String table, Column column, long amount);
}
