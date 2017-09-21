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

import com.google.common.net.HostAndPort;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.region.RegionManager;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class TiSession {
  private static final Map<String, ManagedChannel> connPool = new HashMap<>();
  private final TiConfiguration conf;
  private final RegionManager regionManager;
  private final PDClient client;
  private Catalog catalog; // Meta loading is heavy, pending for lazy loading

  public TiSession(TiConfiguration conf) {
    this.conf = conf;
    this.client = PDClient.createRaw(this);
    this.regionManager = new RegionManager(this.client);
  }

  public TiConfiguration getConf() {
    return conf;
  }

  public TiTimestamp getTimestamp() {
    return client.getTimestamp();
  }

  public Snapshot createSnapshot() {
    return new Snapshot(getTimestamp(), this);
  }

  public Snapshot createSnapshot(TiTimestamp ts) {
    return new Snapshot(ts, this);
  }

  public PDClient getPDClient() {
    return client;
  }

  public synchronized Catalog getCatalog() {
    if (catalog == null) {
      catalog = new Catalog(() -> createSnapshot(),
                                  conf.getMetaReloadPeriod(),
                                  conf.getMetaReloadPeriodUnit());
    }
    return catalog;
  }

  public RegionManager getRegionManager() {
    return regionManager;
  }

  public synchronized ManagedChannel getChannel(String addressStr) {
    ManagedChannel channel = connPool.get(addressStr);
    if (channel == null) {
      HostAndPort address;
      try {
        address = HostAndPort.fromString(addressStr);
      } catch (Exception e) {
        throw new IllegalArgumentException("failed to form address");
      }
      // Channel should be lazy without actual connection until first call
      // So a coarse grain lock is ok here
      channel = ManagedChannelBuilder.forAddress(address.getHostText(), address.getPort())
          .maxInboundMessageSize(conf.getMaxFrameSize())
          .usePlaintext(true)
          .idleTimeout(60, TimeUnit.SECONDS)
          .build();
      connPool.put(addressStr, channel);
    }
    return channel;
  }

  public static TiSession create(TiConfiguration conf) {
    return new TiSession(conf);
  }
}
