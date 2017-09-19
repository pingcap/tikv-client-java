/*
 *
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
 *
 */

package com.pingcap.tikv.region;

import static com.pingcap.tikv.util.KeyRangeUtils.makeRange;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.ReadOnlyPDClient;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.kvproto.Kvrpcpb.IsolationLevel;
import com.pingcap.tikv.kvproto.Metapb.Peer;
import com.pingcap.tikv.kvproto.Metapb.Region;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.kvproto.Metapb.StoreState;
import com.pingcap.tikv.util.Comparables;
import com.pingcap.tikv.util.Pair;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.ParametersAreNonnullByDefault;
import org.apache.log4j.Logger;

public class RegionManager {
  private static final Logger logger = Logger.getLogger(RegionManager.class);
  private final ReadOnlyPDClient pdClient;
  private final LoadingCache<Long, TiRegion> regionCache;
  private final LoadingCache<Long, Store>    storeCache;
  private final RangeMap<Comparable, Long>   keyToRegionIdCache;
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock storeLock = new ReentrantReadWriteLock();

  private static final int MAX_CACHE_CAPACITY = 4096;

  // To avoid double retrieval, we used the async version of grpc
  // When rpc not returned, instead of call again, it wait for previous one done
  public RegionManager(ReadOnlyPDClient pdClient) {
    this.pdClient = pdClient;
    regionCache =
        CacheBuilder.newBuilder()
            .maximumSize(MAX_CACHE_CAPACITY)
            .build(
                new CacheLoader<Long, TiRegion>() {
                  @ParametersAreNonnullByDefault
                  public TiRegion load(Long key) {
                    return pdClient.getRegionByID(key);
                  }
                });

    storeCache =
        CacheBuilder.newBuilder()
            .maximumSize(MAX_CACHE_CAPACITY)
            .build(
                new CacheLoader<Long, Store>() {
                  @ParametersAreNonnullByDefault
                  public Store load(Long id) {
                    return pdClient.getStore(id);
                  }
                });
    keyToRegionIdCache = TreeRangeMap.create();
  }

  public TiSession getSession() {
    return pdClient.getSession();
  }

  public TiRegion getRegionByKey(ByteString key) {
    Long regionId;
    lock.readLock().lock();
    try {
      regionId = keyToRegionIdCache.get(Comparables.wrap(key));
    } finally {
      lock.readLock().unlock();
    }

    if (regionId == null) {
      TiRegion region = pdClient.getRegionByKey(key);
      if (!putRegion(region)) {
        throw new TiClientInternalException("Invalid Region: " + region.toString());
      }
      return region;
    }
    return getRegionById(regionId);
  }

  @SuppressWarnings("unchecked")
  /**
   * Remotes region associated with regionId from regionCache.
   */
  public void invalidateRegion(long regionId) {
    lock.writeLock().lock();
    try {
      TiRegion region = regionCache.get(regionId);
      keyToRegionIdCache.remove(makeRange(region.getStartKey(), region.getEndKey()));
    } catch (Exception ignore) {
    } finally {
      regionCache.invalidate(regionId);
      lock.writeLock().unlock();
    }
  }

  public void invalidateStore(long storeId) {
    storeLock.writeLock().lock();
    storeCache.invalidate(storeId);
    storeLock.writeLock().unlock();
  }

  public Pair<TiRegion, Store> getRegionStorePairByKey(ByteString key) {
    TiRegion region = getRegionByKey(key);
    if (!region.isValid()) {
      throw new TiClientInternalException("Region invalid: " + region.toString());
    }
    Peer leader = region.getLeader();
    long storeId = leader.getStoreId();
    return Pair.create(region, getStoreById(storeId));
  }

  public TiRegion getRegionById(long id) {
    try {
      lock.readLock().lock();
      return regionCache.get(id);
    } catch (Exception e) {
      throw new GrpcException(e);
    } finally {
      lock.readLock().unlock();
    }
  }

  public Store getStoreById(long id) {
    try {
      storeLock.readLock().lock();
      Store store = storeCache.get(id);
      storeLock.readLock().unlock();
      if (store.getState().equals(StoreState.Tombstone)) {
        return null;
      }
      return store;
    } catch (Exception e) {
      throw new GrpcException(e);
    }
  }


  @SuppressWarnings("unchecked")
  private boolean putRegion(TiRegion region) {
    if (!region.hasStartKey() || !region.hasEndKey()) return false;

    lock.writeLock().lock();
    try {
      regionCache.put(region.getId(), region);
      keyToRegionIdCache.put(makeRange(region.getStartKey(), region.getEndKey()), region.getId());
    } finally {
      lock.writeLock().unlock();
    }
    return true;
  }

  public void onRegionStale(long regionID, List<Region> regions) {
    invalidateRegion(regionID);
    regions.stream().map(r -> new TiRegion(r, r.getPeers(0), IsolationLevel.RC)).forEach(this::putRegion);
  }

  public void updateLeader(long regionID, long storeID) {
    try {
      logger.debug(String.format("Thread %s: updateLeader with region id %d",
                                 Thread.currentThread().getId(), regionID));
      lock.readLock().lock();
      Optional<TiRegion> region = Optional.of(regionCache.get(regionID));
      lock.readLock().unlock();
      region.ifPresent(
          r -> {
            if (!r.switchPeer(storeID)) {
              // drop region cache using verID
              logger.warn(String.format("Thread %s: updateLeader failed with region id %d",
                                         Thread.currentThread().getId(), regionID));
              invalidateRegion(regionID);
            }
            logger.debug(String.format("Thread %s: leaving peer switching to %d with region id %d",
                                        Thread.currentThread().getId(),
                                        storeID,
                                        regionID));
          });
    } catch (ExecutionException e) {
      lock.readLock().unlock();
      invalidateRegion(regionID);
    }
  }

  /**
   * Clears all cache when a TiKV server does not respond
   * @param regionID region's id
   * @param storeID TiKV store's id
   */
  public void onRequestFail(long regionID, long storeID) {
    lock.readLock().lock();
    Optional<TiRegion> region = Optional.ofNullable(regionCache.getIfPresent(regionID));
    lock.readLock().unlock();
    region.ifPresent(
        r -> {
          if (!r.onRequestFail(storeID)) {
            invalidateRegion(regionID);
          }
        });
  }
}
