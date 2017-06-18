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

package com.pingcap.tikv.operation;


import com.google.protobuf.ByteString;
import com.pingcap.tikv.RegionManager;
import com.pingcap.tikv.RegionStoreClient;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.grpc.Kvrpcpb;
import com.pingcap.tikv.grpc.Metapb;
import com.pingcap.tikv.meta.TiRange;
import com.pingcap.tikv.util.Pair;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

public class ScanIterator implements Iterator<Kvrpcpb.KvPair> {
    private final TiRange<ByteString> keyRange;
    private final int                           batchSize;
    protected final TiSession                     session;
    private final RegionManager                 regionCache;
    protected final long                          version;

    private List<Kvrpcpb.KvPair>                  currentCache;
    protected ByteString                            startKey;
    protected int                                   index = -1;
    private boolean                               eof = false;

    public ScanIterator(ByteString startKey,
                        int batchSize,
                        TiRange<ByteString> range,
                        TiSession session,
                        RegionManager rm,
                        long version) {
        this.startKey = startKey;
        this.batchSize = batchSize;
        this.keyRange = range;
        this.session = session;
        this.regionCache = rm;
        this.version = version;
    }

    private boolean loadCache() {
        if (eof) return false;

        Pair<Metapb.Region, Metapb.Store> pair = regionCache.getRegionStorePairByKey(startKey);
        Metapb.Region region = pair.first;
        Metapb.Store store = pair.second;
        try (RegionStoreClient client =
                     RegionStoreClient.create(region, store, session)) {
            currentCache = client.scan(startKey, version);
            if (currentCache == null || currentCache.size() == 0) {
                return false;
            }
            index = 0;
            // Session should be single-threaded itself
            // so that we don't worry about conf change in the middle
            // of a transaction. Otherwise below code might lose data
            if (currentCache.size() < batchSize) {
                // Current region done, start new batch from next region
                startKey = region.getEndKey();
                if (startKey.size() == 0 || !contains(startKey)) {
                    return false;
                }
            } else {
                // Start new scan from exact next key in current region
                ByteString lastKey = currentCache.get(currentCache.size() - 1).getKey();
                startKey = KeyUtils.getNextKeyInByteOrder(lastKey);
            }
        } catch (Exception e) {
            throw new TiClientInternalException("Error Closing Store client.", e);
        }
        return true;
    }

    @Override
    public boolean hasNext() {
        if (eof) {
            return false;
        }
        if (index == -1 || index >= currentCache.size()) {
            if (!loadCache()) {
                eof = true;
                return false;
            }
        }
        if (!contains(currentCache.get(index).getKey())) {
            eof = true;
            return false;
        }
        return true;
    }

    private boolean contains(ByteString key) {
        Optional<TiRange<ByteString>> opt = Optional.ofNullable(this.keyRange);
        return !(opt.isPresent() &&
                !opt.get().contains(key));
    }

    private Kvrpcpb.KvPair getCurrent() {
        if (eof) {
            throw new NoSuchElementException();
        }
        if (index < currentCache.size()) {
            Kvrpcpb.KvPair kv = currentCache.get(index++);
            if (!contains(kv.getKey())) {
                eof = true;
                throw new NoSuchElementException();
            }
            return kv;
        }
        return null;
    }

    @Override
    public Kvrpcpb.KvPair next() {
        Kvrpcpb.KvPair kv = getCurrent();
        if (kv == null) {
            // cache drained
            if (!loadCache()) {
                return null;
            }
            return getCurrent();
        }
        return kv;
    }
}
