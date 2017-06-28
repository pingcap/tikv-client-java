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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.grpc.Metapb.Region;
import com.pingcap.tikv.grpc.Metapb.Store;
import com.pingcap.tikv.grpc.Metapb.Peer;
import com.pingcap.tikv.util.Pair;

import javax.annotation.ParametersAreNonnullByDefault;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class RegionManager {
    private final ReadOnlyPDClient pdClient;
    private final LoadingCache<Long, Future<Region>> regionCache;
    private final LoadingCache<Long, Future<Store>> storeCache;
    private final RangeMap<ByteBuffer, Long> keyToRegionIdCache;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private static RegionManager instance;

    public static final int MAX_CACHE_CAPACITY = 4096;

    // To avoid double retrieval, we used the async version of grpc
    // When rpc not returned, instead of call again, it wait for previous one done
    private RegionManager(ReadOnlyPDClient pdClient) {
        this.pdClient = pdClient;
        regionCache = CacheBuilder.newBuilder()
                .maximumSize(MAX_CACHE_CAPACITY)
                .build(new CacheLoader<Long, Future<Region>>() {
                    @ParametersAreNonnullByDefault
                    public Future<Region> load(Long key) {
                        return pdClient.getRegionByIDAsync(key);
                    }
                });

        storeCache = CacheBuilder.newBuilder()
                .maximumSize(MAX_CACHE_CAPACITY)
                .build(new CacheLoader<Long, Future<Store>>() {
                    @ParametersAreNonnullByDefault
                    public Future<Store> load(Long id) {
                        return pdClient.getStoreAsync(id);
                    }
                });
        keyToRegionIdCache =  TreeRangeMap.create();
    }

    public static RegionManager getInstance(TiSession session) {
       if (instance == null)  {
           synchronized (RegionManager.class) {
               if (instance == null) {
                   PDClient pdClient = PDClient.createRaw(session);
                  instance = new RegionManager(pdClient);
               }
           }
       }
       return instance;
    }

    public TiSession getSession() {
        return pdClient.getSession();
    }

    public Region getRegionByKey(ByteString key) {
        Long regionId;
        lock.readLock().lock();
        try {
            regionId = keyToRegionIdCache.get(key.asReadOnlyByteBuffer());
        } finally {
            lock.readLock().unlock();
        }

        if (regionId == null) {
            Region region = pdClient.getRegionByKey(key);
            if (!putRegion(region)) {
                throw new TiClientInternalException("Invalid Region: " + region.toString());
            }
            return region;
        }
        return getRegionById(regionId);
    }


    public void invalidateRegion(long regionId) {
        lock.writeLock().lock();
        try {
            Region region = regionCache.getUnchecked(regionId).get();
            keyToRegionIdCache.remove(makeRange(region.getStartKey(),
                                                region.getEndKey()));
        } catch (Exception ignore) {
        } finally {
            lock.writeLock().unlock();
            regionCache.invalidate(regionId);
        }
    }

    public void invalidateStore(long storeId) {
        storeCache.invalidate(storeId);
    }

    public Pair<Region, Store> getRegionStorePairByKey(ByteString key) {
        Region region = getRegionByKey(key);
        if (!ifValidRegion(region)) {
            throw new TiClientInternalException("Region invalid: " + region.toString());
        }
        Peer leader = region.getPeers(0);
        long storeId = leader.getStoreId();
        return Pair.create(region, getStoreById(storeId));
    }

    public Region getRegionById(long id) {
        try {
            return regionCache.getUnchecked(id).get();
        } catch (Exception e) {
            throw new GrpcException(e);
        }
    }

    public Store getStoreById(long id) {
        try {
            return storeCache.getUnchecked(id).get();
        } catch (Exception e) {
            throw new GrpcException(e);
        }
    }

    private boolean ifValidRegion(Region region) {
        return !(region.getPeersCount() == 0 ||
                !region.hasStartKey() ||
                !region.hasEndKey());
    }

    private boolean putRegion(Region region) {
        if (!region.hasStartKey() || !region.hasEndKey()) return false;

        SettableFuture<Region> regionFuture = SettableFuture.create();
        regionFuture.set(region);
        regionCache.put(region.getId(), regionFuture);

        lock.writeLock().lock();
        try {
            keyToRegionIdCache.put(makeRange(region.getStartKey(), region.getEndKey()),
                                             region.getId());
        } finally {
            lock.writeLock().unlock();
        }
        return true;
    }
    public void onRegionStale(long regionID, List<Region> regions) {
        invalidateRegion(regionID);
        regions.forEach(this::putRegion);
    }

    private boolean isRegionLeaderSwitched(Region region, long storeID) {
        return region.getPeersList().stream().anyMatch(
                p -> p.getStoreId() == storeID
        );
    }

    public void updateLeader(long regionID, long storeID) {
        try {
            Region region = regionCache.getUnchecked(regionID).get();
            if(isRegionLeaderSwitched(region, storeID)) {
                invalidateRegion(regionID);
            }
        } catch (InterruptedException | ExecutionException e) {
            //            e.printStackTrace();
        }
    }

    private static Range<ByteBuffer> makeRange(ByteString startKey, ByteString endKey) {
        return Range.closedOpen(startKey.asReadOnlyByteBuffer(),
                                endKey.asReadOnlyByteBuffer());
    }
}
