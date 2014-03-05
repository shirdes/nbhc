package org.wizbang.hbase.nbhc;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.wizbang.hbase.nbhc.response.RemoteError;

public class RemoteErrorUtil {

    public static final RemoteErrorUtil INSTANCE = new RemoteErrorUtil();

    private static final Logger log = LogManager.getLogger(RemoteErrorUtil.class);

    private RemoteErrorUtil() { }

    public boolean isDoNotRetryError(RemoteError error) {
        Class<?> clazz;
        try {
            clazz = Class.forName(error.getErrorClass());
        }
        catch (ClassNotFoundException e) {
            log.warn("Received remote error containing class that is unable to be loaded on the client side - " + error.getErrorClass(), e);
            return false;
        }

        return DoNotRetryIOException.class.isAssignableFrom(clazz);
    }

    public Exception constructRemoteException(RemoteError error) {
        return new RemoteException(error.getErrorClass(),
                error.getErrorMessage().isPresent() ? error.getErrorMessage().get() : "");
    }
}
