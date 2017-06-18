/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.KeyRange;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.grpc.Kvrpcpb;
import com.pingcap.tikv.grpc.Metapb;
import com.pingcap.tikv.meta.TiRange;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class RegionStoreClientTest {
    private KVMockServer server;
    private static final String LOCAL_ADDR = "127.0.0.1";
    private int port;
    private TiSession session;
    private Metapb.Region region;

    @Before
    public void setUp() throws Exception {
        region = Metapb.Region.newBuilder()
                .setRegionEpoch(Metapb.RegionEpoch.newBuilder()
                                    .setConfVer(1)
                                    .setVersion(2))
                .setId(233)
                .setStartKey(ByteString.EMPTY)
                .setEndKey(ByteString.EMPTY)
                .addPeers(Metapb.Peer.newBuilder()
                                    .setId(11)
                                    .setStoreId(13))
                .build();
        server = new KVMockServer();
        port = server.start(region);
        // No PD needed in this test
        TiConfiguration conf = TiConfiguration.createDefault(ImmutableList.of(""));
        session = TiSession.create(conf);
    }

    private RegionStoreClient createClient() {
        Metapb.Store store = Metapb.Store.newBuilder()
                .setAddress(LOCAL_ADDR + ":" + port)
                .setId(1)
                .setState(Metapb.StoreState.Up)
                .build();

        return RegionStoreClient.create(region, store, session);
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void get() throws Exception {
        RegionStoreClient client = createClient();
        server.put("key1", "value1");
        ByteString value = client.get(ByteString.copyFromUtf8("key1"), 1);
        assertEquals(ByteString.copyFromUtf8("value1"), value);

        server.putError("error1", KVMockServer.ABORT);
        try {
            client.get(ByteString.copyFromUtf8("error1"), 1);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }
        server.clearAllMap();
        client.close();
    }

    @Test
    public void getAsync() throws Exception {
        RegionStoreClient client = createClient();

        server.put("key1", "value1");
        Future<ByteString> value = client.getAsync(ByteString.copyFromUtf8("key1"), 1);
        assertEquals(value.get(), ByteString.copyFromUtf8("value1"));

        boolean futureSet = false;
        server.putError("error1", KVMockServer.ABORT);
        try {
            value = client.getAsync(ByteString.copyFromUtf8("error1"), 1);
            futureSet = true;
            value.get();
            fail();
        } catch (Exception e) {
            assertTrue(futureSet);
        }
        server.clearAllMap();
        client.close();
    }

    @Test
    public void batchGetAsync() throws Exception {
        RegionStoreClient client = createClient();

        server.put("key1", "value1");
        server.put("key2", "value2");
        server.put("key4", "value4");
        server.put("key5", "value5");
        Future<List<Kvrpcpb.KvPair>> kvsFuture =
                client.batchGetAsync(ImmutableList.of(ByteString.copyFromUtf8("key1"),
                                                 ByteString.copyFromUtf8("key2")), 1);
        List<Kvrpcpb.KvPair> kvs = kvsFuture.get();
        assertEquals(2, kvs.size());
        kvs.forEach(kv -> assertEquals(kv.getKey().toStringUtf8().replace("key", "value"),
                                       kv.getValue().toStringUtf8()));

        server.putError("error1", KVMockServer.ABORT);
        boolean futureSet = false;
        try {
            kvsFuture = client.batchGetAsync(ImmutableList.of(ByteString.copyFromUtf8("key1"),
                                             ByteString.copyFromUtf8("error1")), 1);
            futureSet = true;
            kvsFuture.get();
            fail();
        } catch (Exception e) {
            assertTrue(futureSet);
        }
        server.clearAllMap();
        client.close();
    }

    @Test
    public void batchGet() throws Exception {
        RegionStoreClient client = createClient();

        server.put("key1", "value1");
        server.put("key2", "value2");
        server.put("key4", "value4");
        server.put("key5", "value5");
        List<Kvrpcpb.KvPair> kvs =
                client.batchGet(ImmutableList.of(ByteString.copyFromUtf8("key1"),
                        ByteString.copyFromUtf8("key2")), 1);
        assertEquals(2, kvs.size());
        kvs.forEach(kv -> assertEquals(kv.getKey().toStringUtf8().replace("key", "value"),
                kv.getValue().toStringUtf8()));

        server.putError("error1", KVMockServer.ABORT);
        try {
            client.batchGet(ImmutableList.of(ByteString.copyFromUtf8("key1"),
                    ByteString.copyFromUtf8("error1")), 1);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }
        server.clearAllMap();
        client.close();
    }

    @Test
    public void scan() throws Exception {
        RegionStoreClient client = createClient();

        server.put("key1", "value1");
        server.put("key2", "value2");
        server.put("key4", "value4");
        server.put("key5", "value5");
        List<Kvrpcpb.KvPair> kvs =
                client.scan(ByteString.copyFromUtf8("key2"), 1);
        assertEquals(3, kvs.size());
        kvs.forEach(kv -> assertEquals(kv.getKey().toStringUtf8().replace("key", "value"),
                kv.getValue().toStringUtf8()));

        server.putError("error1", KVMockServer.ABORT);
        try {
            client.scan(ByteString.copyFromUtf8("error1"), 1);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }
        server.clearAllMap();
        client.close();
    }

    @Test
    public void scanAsync() throws Exception {
        RegionStoreClient client = createClient();

        server.put("key1", "value1");
        server.put("key2", "value2");
        server.put("key4", "value4");
        server.put("key5", "value5");
        Future<List<Kvrpcpb.KvPair>> kvsFuture =
                client.scanAsync(ByteString.copyFromUtf8("key2"), 1);
        List<Kvrpcpb.KvPair> kvs = kvsFuture.get();
        assertEquals(3, kvs.size());
        kvs.forEach(kv -> assertEquals(kv.getKey().toStringUtf8().replace("key", "value"),
                kv.getValue().toStringUtf8()));

        server.putError("error1", KVMockServer.ABORT);
        boolean futureSet = false;
        try {
            kvsFuture = client.scanAsync(ByteString.copyFromUtf8("error1"), 1);
            futureSet = true;
            kvsFuture.get();
            fail();
        } catch (Exception e) {
            assertTrue(futureSet);
        }
        server.clearAllMap();
        client.close();
    }

    @Test
    public void coprocess() throws Exception {
        RegionStoreClient client = createClient();

        server.put("key1", "value1");
        server.put("key2", "value2");
        server.put("key4", "value4");
        server.put("key5", "value5");
        server.put("key6", "value6");
        server.put("key7", "value7");
        SelectRequest.Builder builder = SelectRequest.newBuilder();
        builder.setStartTs(1);
        List<TiRange<ByteString>> tiRanges = ImmutableList.of(
                TiRange.createByteStringRange(ByteString.copyFromUtf8("key1"), ByteString.copyFromUtf8("key4")),
                TiRange.createByteStringRange(ByteString.copyFromUtf8("key6"), ByteString.copyFromUtf8("key7")));

        Iterable<KeyRange> keyRanges = tiRanges.stream().map(range -> KeyRange
                .newBuilder()
                .setLow(range.getLowValue())
                .setHigh(range.getHighValue())
                .build()).collect(Collectors.toList());


        builder.addAllRanges(keyRanges);
        SelectResponse resp =
                client.coprocess(builder.build(), tiRanges);
        assertEquals(5, resp.getChunksCount());
        Set<String> results = ImmutableSet.copyOf(
                resp.getChunksList().stream().map(c -> c.getRowsData().toStringUtf8()).collect(Collectors.toList())
        );
        assertTrue(ImmutableList.of("value1", "value2", "value4", "value6", "value7").stream().allMatch(results::contains));


        builder = SelectRequest.newBuilder();
        builder.setStartTs(1);
        tiRanges = ImmutableList.of(
                TiRange.createByteStringRange(ByteString.copyFromUtf8("error1"), ByteString.copyFromUtf8("error2")));

        keyRanges = tiRanges.stream().map(range -> KeyRange
                .newBuilder()
                .setLow(range.getLowValue())
                .setHigh(range.getHighValue())
                .build()).collect(Collectors.toList());
        builder.addAllRanges(keyRanges);

        server.putError("error1", KVMockServer.ABORT);
        try {
            client.coprocess(builder.build(), tiRanges);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }
        server.clearAllMap();
        client.close();
    }

    @Test
    public void coprocessAsync() throws Exception {
        RegionStoreClient client = createClient();

        server.put("key1", "value1");
        server.put("key2", "value2");
        server.put("key4", "value4");
        server.put("key5", "value5");
        server.put("key6", "value6");
        server.put("key7", "value7");
        SelectRequest.Builder builder = SelectRequest.newBuilder();
        builder.setStartTs(1);
        List<TiRange<ByteString>> tiRanges = ImmutableList.of(
                TiRange.createByteStringRange(ByteString.copyFromUtf8("key1"), ByteString.copyFromUtf8("key4")),
                TiRange.createByteStringRange(ByteString.copyFromUtf8("key6"), ByteString.copyFromUtf8("key7")));

        Iterable<KeyRange> keyRanges = tiRanges.stream().map(range -> KeyRange
                .newBuilder()
                .setLow(range.getLowValue())
                .setHigh(range.getHighValue())
                .build()).collect(Collectors.toList());


        builder.addAllRanges(keyRanges);
        Future<SelectResponse> respFuture =
                client.coprocessAsync(builder.build(), tiRanges);
        SelectResponse resp = respFuture.get();
        assertEquals(5, resp.getChunksCount());
        Set<String> results = ImmutableSet.copyOf(
                resp.getChunksList().stream().map(c -> c.getRowsData().toStringUtf8()).collect(Collectors.toList())
        );
        assertTrue(Iterables.all(ImmutableList.of("value1", "value2", "value4", "value6", "value7"),
                results::contains));


        builder = SelectRequest.newBuilder();
        builder.setStartTs(1);
        tiRanges = ImmutableList.of(
                TiRange.createByteStringRange(ByteString.copyFromUtf8("error1"), ByteString.copyFromUtf8("error2")));

        keyRanges = tiRanges.stream().map(range -> KeyRange
                .newBuilder()
                .setLow(range.getLowValue())
                .setHigh(range.getHighValue())
                .build()).collect(Collectors.toList());
        builder.addAllRanges(keyRanges);

        server.putError("error1", KVMockServer.ABORT);
        boolean futureSet = false;
        try {
            Future<SelectResponse> future = client.coprocessAsync(builder.build(), tiRanges);
            futureSet = true;
            future.get();
            fail();
        } catch (Exception e) {
            assertTrue(futureSet);
        }
        server.clearAllMap();
        client.close();
    }

}