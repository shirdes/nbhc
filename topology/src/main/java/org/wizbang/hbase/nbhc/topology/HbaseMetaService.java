package org.wizbang.hbase.nbhc.topology;

import com.google.common.util.concurrent.Service;

public interface HbaseMetaService extends Service {

    RegionOwnershipTopology getTopology();

}
