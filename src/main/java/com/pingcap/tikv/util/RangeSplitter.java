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

package com.pingcap.tikv.util;


import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.grpc.Coprocessor.KeyRange;
import com.pingcap.tikv.grpc.Metapb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class RangeSplitter {
    public static class RegionTask implements Serializable {
        private final TiRegion region;
        private final Metapb.Store store;
        private final List<KeyRange> ranges;

        public RegionTask(TiRegion region,
                          Metapb.Store store,
                          List<KeyRange> ranges) {
            this.region = region;
            this.store = store;
            this.ranges = ranges;
        }

        public TiRegion getRegion() {
            return region;
        }

        public Metapb.Store getStore() {
            return store;
        }

        public List<KeyRange> getRanges() {
            return ranges;
        }
    }
    public static RangeSplitter newSplitter(RegionManager mgr) {
        return new RangeSplitter(mgr);
    }

    private RangeSplitter(RegionManager regionManager) {
        this.regionManager = regionManager;
    }

    protected final RegionManager regionManager;

    // both arguments represent right side of end points
    // so that empty is +INF
    private static int rightCompareTo(ByteString lhs, ByteString rhs) {
        // both infinite
        if (lhs.isEmpty() && rhs.isEmpty()) {
            return 0;
        }
        if (lhs.isEmpty()) {
            return 1;
        }
        if (rhs.isEmpty()) {
            return -1;
        }
        return lhs.asReadOnlyByteBuffer().compareTo(rhs.asReadOnlyByteBuffer());
    }

    public List<RegionTask> splitRangeByRegion(List<KeyRange> keyRanges) {
        checkArgument(keyRanges != null && keyRanges.size() != 0);
        int i = 0;
        KeyRange range = keyRanges.get(i++);
        Map<Long, List<KeyRange>> idToRange = new HashMap<>(); // region id to keyRange list
        Map<Long, Pair<TiRegion, Metapb.Store>> idToRegion = new HashMap<>();

        while (true) {
            Pair<TiRegion, Metapb.Store> regionStorePair =
                    regionManager.getRegionStorePairByKey(range.getStart());

            requireNonNull(regionStorePair, "fail to get region/store pair by key" + range.getStart());
            TiRegion region = regionStorePair.first;
            idToRegion.putIfAbsent(region.getId(), regionStorePair);

            // both key range is close-opened
            // initial range inside pd is guaranteed to be -INF to +INF
            if (rightCompareTo(range.getEnd(), region.getEndKey()) > 0) {
                // current region does not cover current end key
                KeyRange cutRange = KeyRange.newBuilder()
                                               .setStart(range.getStart())
                                               .setEnd(region.getEndKey())
                                               .build();

                List<KeyRange> ranges = idToRange.computeIfAbsent(region.getId(), k -> new ArrayList());
                ranges.add(cutRange);

                // cut new remaining for current range
                range = KeyRange.newBuilder()
                                .setStart(region.getEndKey())
                                .setEnd(range.getEnd())
                                .build();
            } else {
                // current range covered by region
                List<KeyRange> ranges = idToRange.computeIfAbsent(region.getId(), k -> new ArrayList());
                ranges.add(range);
                if (i >= keyRanges.size()) {
                    break;
                }
                range = keyRanges.get(i++);
            }
        }

        ImmutableList.Builder<RegionTask> resultBuilder = ImmutableList.builder();
        for (Map.Entry<Long, List<KeyRange>> entry : idToRange.entrySet()) {
            Pair<TiRegion, Metapb.Store> regionStorePair = idToRegion.get(entry.getKey());
            resultBuilder.add(new RegionTask(
                    regionStorePair.first,
                    regionStorePair.second,
                    entry.getValue()
            ));
        }
        return resultBuilder.build();
    }
}
