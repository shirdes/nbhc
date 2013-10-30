package com.urbanairship.hbase.shc;

import com.google.common.base.Function;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

import java.lang.reflect.Method;

public final class Protocol {

    public static final Class<? extends VersionedProtocol> TARGET_PROTOCOL = HRegionInterface.class;

    public static final Method GET_TARGET_METHOD = loadTargetMethod("get", new Class[]{byte[].class, Get.class});
    public static final Function<HbaseObjectWritable, Result> GET_RESPONSE_PARSER = new Function<HbaseObjectWritable, Result>() {
        @Override
        public Result apply(HbaseObjectWritable value) {
            Object result = value.get();
            if (!(result instanceof Result)) {
                throw new RuntimeException(String.format("Expected response value of %s but received %s for 'get' operation",
                        Result.class.getName(), result.getClass().getName()));
            }

            return (Result) result;
        }
    };

    public static final Method PUT_TARGET_METHOD = loadTargetMethod("put", new Class[]{byte[].class, Put.class});
    public static final Function<HbaseObjectWritable, Void> PUT_RESPONSE_PARSER = new VoidResponseParser("put");

    public static final Method CHECK_AND_PUT_TARGET_METHOD = loadTargetMethod("checkAndPut", new Class[] {byte[].class, byte[].class, byte[].class, byte[].class, byte[].class, Put.class});
    public static final Function<HbaseObjectWritable, Boolean> CHECK_AND_PUT_RESPONSE_PARSER = new BooleanResponseParser("checkAndPut");

    public static final Method CHECK_AND_DELETE_TARGET_METHOD = loadTargetMethod("checkAndDelete", new Class[] {byte[].class, byte[].class, byte[].class, byte[].class, byte[].class, Delete.class});
    public static final Function<HbaseObjectWritable, Boolean> CHECK_AND_DELETE_RESPONSE_PARSER = new BooleanResponseParser("checkAndDelete");

    public static final Method DELETE_TARGET_METHOD = loadTargetMethod("delete", new Class[]{byte[].class, Delete.class});
    public static final Function<HbaseObjectWritable, Void> DELETE_RESPONSE_PARSER = new VoidResponseParser("delete");

    public static final Method MULTI_ACTION_TARGET_METHOD = loadTargetMethod("multi", new Class[]{MultiAction.class});

    public static final Method OPEN_SCANNER_TARGET_METHOD = loadTargetMethod("openScanner", new Class[]{byte[].class, Scan.class});
    public static final Function<HbaseObjectWritable, Long> OPEN_SCANNER_RESPONSE_PARSER = new Function<HbaseObjectWritable, Long>() {
        @Override
        public Long apply(HbaseObjectWritable value) {
            Object object = value.get();
            if (!(object instanceof Long)) {
                throw new RuntimeException(String.format("Expected response value of %s but received %s for 'open scanner' operation",
                        Long.class.getName(), object.getClass().getName()));
            }

            return (Long) object;
        }
    };

    public static final Method CLOSE_SCANNER_TARGET_METHOD = loadTargetMethod("close", new Class[]{Long.TYPE});
    public static final Function<HbaseObjectWritable, Void> CLOSE_SCANNER_RESPONSE_PARSER = new VoidResponseParser("close scanner");

    public static final Method SCANNER_NEXT_TARGET_METHOD = loadTargetMethod("next", new Class[]{Long.TYPE, Integer.TYPE});

    public static Method loadTargetMethod(String methodName, Class<?>[] params) {
        try {
            return HRegionServer.class.getMethod(methodName, params);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(String.format("Unable to load target method for '%s' operation", methodName));
        }
    }

    private Protocol() { }

    private static final class VoidResponseParser implements Function<HbaseObjectWritable, Void> {

        private final String operationName;

        public VoidResponseParser(String operationName) {
            this.operationName = operationName;
        }

        @Override
        public Void apply(HbaseObjectWritable value) {
            Class<?> resultType = value.getDeclaredClass();
            if (!Void.TYPE.equals(resultType)) {
                throw new RuntimeException(String.format("Expected response value of %s but received %s for '%s' operation",
                        Void.class.getName(), resultType.getName(), operationName));
            }

            return null;
        }
    }

    private static final class BooleanResponseParser implements Function<HbaseObjectWritable, Boolean> {

        private final String operationName;

        public BooleanResponseParser(String operationName) {
            this.operationName = operationName;
        }

        @Override
        public Boolean apply(HbaseObjectWritable value) {
            Object object = value.get();
            if (!(object instanceof Boolean)) {
                throw new RuntimeException(String.format("Expected result of %s but received %s for '%s' operation",
                        Boolean.class.getName(), object.getClass().getName(), operationName));
            }

            return (Boolean) object;
        }
    }
}