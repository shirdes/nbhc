package org.wizbang.hbase.nbhc;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wizbang.hbase.nbhc.request.scan.ScannerResultStream;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;

import static org.apache.commons.lang.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;
import static org.junit.Assert.*;

public class ClientTest {

    private static final String TABLE = "TEST";
    private static final byte[] FAMILY = "f".getBytes(Charsets.UTF_8);
    private static final byte[] COL = "c".getBytes(Charsets.UTF_8);

    private static HbaseClientService clientService;
    private static HbaseClient client;

    @BeforeClass
    public static void setUp() throws Exception {
        clientService = HbaseClientFactory.create(new HbaseClientConfiguration());
        clientService.startAndWait();
        client = clientService.getClient();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        clientService.stopAndWait();
    }

    @Test
    public void testSingleCrud() throws Exception {
        String row = UUID.randomUUID().toString();
        String value = randomAlphabetic(5);

        Put put = new Put(Bytes.toBytes(row), System.currentTimeMillis());
        put.add(FAMILY, COL, Bytes.toBytes(value));

        Future<Void> putFuture = client.put(TABLE, put);
        putFuture.get();

        Get get = new Get(Bytes.toBytes(row));

        Future<Result> future = client.get(TABLE, get);

        Result result = future.get();

        assertTrue(result != null && !result.isEmpty());

        byte[] column = result.getValue(FAMILY, COL);
        assertTrue(column != null && column.length > 0);

        assertEquals(value, Bytes.toString(column));

        Delete delete = new Delete(Bytes.toBytes(row), System.currentTimeMillis() + 1L, null);
        Future<Void> deleteFuture = client.delete(TABLE, delete);
        deleteFuture.get();

        future = client.get(TABLE, get);
        result = future.get();

        assertTrue(result.isEmpty());
    }

    @Test
    public void testMulti() throws Exception {
        Map<String, String> entries = Maps.newHashMap();
        for (int i = 0; i < 100; i++) {
            entries.put(UUID.randomUUID().toString(), randomAlphanumeric(5));
        }

        ImmutableList.Builder<Put> puts = ImmutableList.builder();
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            byte[] key = distributedKey(entry.getKey());
            Put put = new Put(key, System.currentTimeMillis());
            put.add(FAMILY, COL, Bytes.toBytes(entry.getValue()));

            puts.add(put);
        }

        ListenableFuture<Void> future = client.multiPut(TABLE, puts.build());
        future.get();

        ImmutableList.Builder<Get> builder = ImmutableList.builder();
        for (String key : entries.keySet()) {
            builder.add(new Get(distributedKey(key)));
        }

        ImmutableList<Get> gets = builder.build();

        ListenableFuture<ImmutableList<Result>> getFuture = client.multiGet(TABLE, gets);
        List<Result> results = getFuture.get();

        assertEquals(entries.size(), results.size());
        for (int i = 0; i < results.size(); i++) {
            Result result = results.get(i);

            assertArrayEquals(gets.get(i).getRow(), result.getRow());

            String key = parseKey(result.getRow());
            assertEquals(entries.get(key), Bytes.toString(result.getValue(FAMILY, COL)));
        }

        Set<String> found = new HashSet<String>();
        Scan scan = new Scan();
        scan.addFamily(FAMILY);
        scan.setCaching(10);
        ScannerResultStream stream = client.getScannerStream(TABLE, scan);
        try {
            while (stream.hasNext()) {
                Result result = stream.next();

                String key = parseKey(result.getRow());
                if (!entries.containsKey(key)) continue;

                assertEquals(1, result.getFamilyMap(FAMILY).size());
                assertEquals(entries.get(key), Bytes.toString(result.getValue(FAMILY, COL)));

                found.add(key);
            }
        }
        finally {
            stream.close();
        }

        assertEquals(ImmutableSet.copyOf(entries.keySet()), ImmutableSet.copyOf(found));

        List<String> rows = Lists.newArrayList(entries.keySet());
        ImmutableList.Builder<Delete> deletes = ImmutableList.builder();

        Set<String> removed = Sets.newHashSet();
        for (int i = 0; i < 3; i++) {
            int idx = RandomUtils.nextInt(rows.size());
            String row = rows.get(idx);
            byte[] key = distributedKey(row);
            deletes.add(new Delete(key, System.currentTimeMillis() + 1L, null));

            removed.add(row);
        }

        ListenableFuture<Void> deleteFuture = client.multiDelete(TABLE, deletes.build());
        deleteFuture.get();

        getFuture = client.multiGet(TABLE, gets);
        results = getFuture.get();

        assertEquals(entries.size(), results.size());
        for (int i = 0; i < results.size(); i++) {
            Result result = results.get(i);

            String key;
            if (result.isEmpty()) {
                Get correspondingGet = gets.get(i);
                key = parseKey(correspondingGet.getRow());
            }
            else {
                key = parseKey(result.getRow());
            }

            if (removed.contains(key)) {
                assertTrue(result.isEmpty());
            }
            else {
                assertEquals(entries.get(key), Bytes.toString(result.getValue(FAMILY, COL)));
            }
        }
    }

    private String parseKey(byte[] row) {
        return new String(Arrays.copyOfRange(row, 2, row.length), Charsets.UTF_8);
    }

    private byte[] distributedKey(String base) {
        byte[] baseBytes = base.getBytes(Charsets.UTF_8);

        byte partition = (byte) Character.digit(base.charAt(0), 16);

        byte[] key = new byte[2 + baseBytes.length];
        ByteBuffer.wrap(key)
                .put(partition)
                .put((byte) ':')
                .put(baseBytes);

        return key;
    }

    @Test
    public void testCheckedActions() throws Exception {
        byte[] row = UUID.randomUUID().toString().getBytes(Charsets.UTF_8);
        byte[] column = randomAlphanumeric(10).getBytes(Charsets.UTF_8);

        ColumnCheck check = ColumnCheck.newBuilder()
                .setRow(row)
                .setFamily(FAMILY)
                .setQualifier(column)
                .setValueNotPresent()
                .build();

        byte[] value = randomAlphanumeric(5).getBytes(Charsets.UTF_8);

        Put put = new Put(row, System.currentTimeMillis());
        put.add(FAMILY, column, value);

        ListenableFuture<Boolean> future = client.checkAndPut(TABLE, check, put);
        Boolean result = future.get();

        assertTrue(result);

        byte[] failedValue = randomAlphanumeric(6).getBytes(Charsets.UTF_8);
        put = new Put(row, System.currentTimeMillis() + 1L);
        put.add(FAMILY, column, failedValue);

        future = client.checkAndPut(TABLE, check, put);
        result = future.get();

        assertFalse(result);

        Delete delete = new Delete(row, System.currentTimeMillis() + 2L, null);

        future = client.checkAndDelete(TABLE, check, delete);
        result = future.get();

        assertFalse(result);

        ColumnCheck deleteCheck = ColumnCheck.newBuilder()
                .setRow(row)
                .setFamily(FAMILY)
                .setQualifier(column)
                .setRequiredValue(value)
                .build();

        future = client.checkAndDelete(TABLE, deleteCheck, delete);
        result = future.get();

        assertTrue(result);
    }

    @Test
    public void testIncrement() throws Exception {
        byte[] qualifier = randomAlphanumeric(10).getBytes(Charsets.UTF_8);

        Column column = Column.newBuilder()
                .setRow(UUID.randomUUID().toString().getBytes(Charsets.UTF_8))
                .setFamily(FAMILY)
                .setQualifier(qualifier)
                .build();

        ListenableFuture<Long> future = client.incrementColumnValue(TABLE, column, 7);
        Long result = future.get();

        assertEquals(7, result.longValue());

        future = client.incrementColumnValue(TABLE, column, 17);
        result = future.get();

        assertEquals(17 + 7, result.longValue());
    }
}
