package com.urbanairship.hbase.shc.request;

import com.urbanairship.hbase.shc.dispatch.ResultBroker;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;

public interface ResponseProcessor<R> {

    void process(HRegionLocation location, HbaseObjectWritable received, ResultBroker<R> resultBroker);

}
