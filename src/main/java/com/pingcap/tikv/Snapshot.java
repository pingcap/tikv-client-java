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
import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.grpc.Kvrpcpb.KvPair;
import com.pingcap.tikv.grpc.Metapb.Store;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.operation.IndexScanIterator;
import com.pingcap.tikv.operation.ScanIterator;
import com.pingcap.tikv.operation.SelectIterator;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.Comparables;
import com.pingcap.tikv.util.Pair;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.pingcap.tikv.util.KeyRangeUtils.makeRange;

public class Snapshot {
    private final Version version;
    private final RegionManager regionCache;
    private final TiSession session;
    private static final int EPOCH_SHIFT_BITS = 18;
    private final TiConfiguration conf;

    public Snapshot(Version version, RegionManager regionCache, TiSession session) {
        this.version = version;
        this.regionCache = regionCache;
        this.session = session;
        this.conf = session.getConf();
    }

    public Snapshot(RegionManager regionCache, TiSession session) {
        this(Version.getCurrentTSAsVersion(), regionCache, session);
    }

    public TiSession getSession() {
        return session;
    }

    public long getVersion() {
        return version.getVersion();
    }

    public byte[] get(byte[] key) {
        ByteString keyString = ByteString.copyFrom(key);
        ByteString value = get(keyString);
        return value.toByteArray();
    }

    public ByteString get(ByteString key) {
        Pair<TiRegion, Store> pair = regionCache.getRegionStorePairByKey(key);
        RegionStoreClient client = RegionStoreClient.create(pair.first, pair.second, getSession(), regionCache);
        // TODO: Need to deal with lock error after grpc stable
        return client.get(key, version.getVersion());
    }

    /**
     * Issue a select request to TiKV and PD.
     *
     * @param selReq is SelectRequest.
     * @return a Iterator that contains all result from this select request.
     */
    public Iterator<Row> select(TiSelectRequest selReq) {
        return new SelectIterator(selReq,
                getSession(),
                regionCache,
                false);
    }

    public Iterator<Row> selectByIndex(TiSelectRequest selReq, boolean singleReadMode) {
        Iterator<Row> iter = new SelectIterator(selReq,
                getSession(),
                regionCache,
                true);
        return IndexScanIterator.of(this, selReq, iter, singleReadMode);
    }

    /**
     * Below is lower level API for env like Spark which already did key range split
     * Perform table scan
     * @param req SelectRequest for coprocessor
     * @param task RegionTask of the coprocessor request to send
     * @return Row iterator to iterate over resulting rows
     */
    public Iterator<Row> select(TiSelectRequest req, RegionTask task) {
        return new SelectIterator(req, ImmutableList.of(task), getSession(), regionCache,false);
    }

    /**
     * Below is lower level API for env like Spark which already did key range split
     * Perform index double read
     * @param req SelectRequest for coprocessor
     * @param task RegionTask of the coprocessor request to send
     * @return Row iterator to iterate over resulting rows
     */
    public Iterator<Row> selectByIndex(TiSelectRequest req, RegionTask task, boolean singleReadmode) {
        Iterator<Row> iter = new SelectIterator(req, ImmutableList.of(task), getSession(), regionCache,true);
        return IndexScanIterator.of(this, req, iter, singleReadmode);
    }

    public Iterator<KvPair> scan(ByteString startKey) {
        return new ScanIterator(
                startKey, conf.getScanBatchSize(), null, session, regionCache, version.getVersion());
    }

    // TODO: Need faster implementation, say concurrent version
    // Assume keys sorted
    public List<KvPair> batchGet(List<ByteString> keys) {
        TiRegion curRegion = null;
        Range curKeyRange = null;
        Pair<TiRegion, Store> lastPair = null;
        List<ByteString> keyBuffer = new ArrayList<>();
        List<KvPair> result = new ArrayList<>(keys.size());
        for (ByteString key : keys) {
            if (curRegion == null || !curKeyRange.contains(Comparables.wrap(key))) {
                Pair<TiRegion, Store> pair = regionCache.getRegionStorePairByKey(key);
                lastPair = pair;
                curRegion = pair.first;
                curKeyRange = makeRange(curRegion.getStartKey(), curRegion.getEndKey());

                try (RegionStoreClient client = RegionStoreClient.create(lastPair.first, lastPair.second, getSession(), regionCache)) {
                    List<KvPair> partialResult = client.batchGet(keyBuffer, version.getVersion());
                    // TODO: Add lock check
                    result.addAll(partialResult);
                } catch (Exception e) {
                    throw new TiClientInternalException("Error Closing Store client.", e);
                }
                keyBuffer = new ArrayList<>();
                keyBuffer.add(key);
            }
        }
        return result;
    }

    public static class Version {
        public static Version getCurrentTSAsVersion() {
            long t = System.currentTimeMillis() << EPOCH_SHIFT_BITS;
            return new Version(t);
        }

        private final long version;

        private Version(long ts) {
            version = ts;
        }

        public long getVersion() {
            return version;
        }
    }
}
