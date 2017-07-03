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

package com.pingcap.tikv;


import com.google.protobuf.ByteString;
import com.pingcap.tikv.grpc.Kvrpcpb;
import com.pingcap.tikv.grpc.Metapb;
import com.pingcap.tikv.grpc.Metapb.Peer;
import com.pingcap.tikv.grpc.Metapb.Region;

import java.util.Set;

public class TiRegion {
    private Region meta;
    private Peer peer;
    private Set<Long> unreachableStores;

    public TiRegion(Region meta, Peer peer) {
        this.meta = meta;
        this.peer = peer;
    }

    public Peer getLeader() {
        return peer;
    }
    public class RegionVerID {
        long id;
        long confVer;
        long ver;
        public RegionVerID(long id, long confVer, long ver) {
            this.id = id;
            this.confVer = confVer;
            this.ver = ver;
        }
    }

    public long getId() {
        return this.meta.getId();
    }

    public RegionVerID VerID() {
        long id = this.meta.getId();
        long confVer = this.meta.getRegionEpoch().getConfVer();
        long ver = this.meta.getRegionEpoch().getVersion();
        return new RegionVerID(id, confVer, ver);
    }

    public ByteString getStartKey() {
        return meta.getStartKey();
    }

    public ByteString getEndKey() {
        return meta.getEndKey();
    }

    public Kvrpcpb.Context getContext() {
        Kvrpcpb.Context.Builder builder = Kvrpcpb.Context.newBuilder();
        builder.setRegionId(meta.getId()).setPeer(this.peer).setRegionEpoch(this.meta.getRegionEpoch());
        return builder.build();
    }

    public boolean onRequestFail(long storeID) {
        if (peer.getStoreId() == storeID) {
            return true;
        }
        //TODO region_cache line 537
        return false;
    }
    /**
     * switches current peer to the one on specific store. It return false if no
     * peer matches the storeID.
     *
     */
    public  boolean switchPeer(long storeID) {
        return meta.getPeersList().stream().anyMatch(
                p -> {
                    if (p.getStoreId() == storeID) {
                        this.peer = p;
                        return true;
                    }
                    return false;
                }
        );
    }

    public boolean contains(ByteString key) {
        return meta.getStartKey().equals(key) && (meta.getEndKey().equals(key) || meta.getEndKey().isEmpty());
    }

    public boolean isValid() {
       return peer != null &&  meta.hasStartKey() && meta.hasEndKey();
    }

    public boolean hasStartKey() {
        return meta.hasStartKey();
    }

    public boolean hasEndKey() {
        return meta.hasEndKey();
    }

    public Metapb.RegionEpoch getRegionEpoch() {
        return this.meta.getRegionEpoch();
    }

    public Region getMeta() {
        return meta;
    }
}
