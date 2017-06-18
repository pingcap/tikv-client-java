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

package com.pingcap.tikv.meta;
import com.google.common.collect.*;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.util.TiFluentIterable;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

// Basically this one should reuse Guava's Range but make it work with
// non-comparable type ByteString
// TODO: Refactor needed
public class TiRange<E> implements Serializable {
    private final E lowVal;
    private final E highVal;
    private final boolean leftOpen;
    private final boolean rightOpen;
    // This is almost a hack for tiSpark
    // In reality comparator needs to be deseraialized as well
    // but in case of TiSpark, we don't do any compare anymore
    transient private Comparator<E> comparator;

    public static <T> TiRange<T> create(T l, T h, boolean lopen, boolean ropen, Comparator<T> c) {
        return new TiRange<T>(l, h, lopen, ropen, c);
    }

    public static TiRange<Long> createLongRange(long l, long h, boolean lOpen, boolean rOpen) {
        return new TiRange(l, h, lOpen, rOpen, Comparator.naturalOrder());
    }

    public static TiRange<Long> createLongPoint(long v) {
        return new TiRange(v, v, false, false, Comparator.naturalOrder());
    }

    public static <T extends Comparable<T>> TiRange<T> create(T l, T h) {
        return new TiRange<T>(l, h, false, true, Comparator.naturalOrder());
    }

    public static <T> TiRange<T> create(T l, T h, Comparator<T> comp) {
        return new TiRange<T>(l, h, false, true, comp);
    }

    public static TiRange<ByteString> createByteStringRange(ByteString l, ByteString h) {
        return new TiRange<>(l, h, false, true, Comparator.comparing(ByteString::asReadOnlyByteBuffer));
    }

    public static TiRange<ByteString> createByteStringRange(ByteString l, ByteString h, boolean lopen, boolean ropen) {
        return new TiRange<>(l, h, lopen, ropen, Comparator.comparing(ByteString::asReadOnlyByteBuffer));
    }

    private TiRange(E l, E h, boolean lopen, boolean ropen, Comparator<E> comp) {
        this.lowVal = l;
        this.highVal = h;
        this.leftOpen = lopen;
        this.rightOpen = ropen;
        this.comparator = comp;
    }

    static public <T extends Comparable<T>> List<TiRange<T>> intersect(List<TiRange<T>> ranges, List<TiRange<T>> others) {
        // TODO: Unify range operations instead of wrap around guava utilities
        // This implementation is just for "make it work"
        RangeSet<T> rangeSet = TreeRangeSet.create();
        for (TiRange<T> range : ranges) {
            rangeSet.add(toGuavaRange(range));
        }

        for (TiRange<T> other : others) {
            rangeSet = rangeSet.subRangeSet(toGuavaRange(other));
        }

        return ImmutableList.copyOf(
                TiFluentIterable
                        .from(rangeSet.asRanges())
                        .transform(TiRange::toTiRange)
        );
    }

    static public <T extends Comparable<T>> List<TiRange<T>> union(List<TiRange<T>> ranges, List<TiRange<T>> others) {
        // TODO: Unify range operations instead of wrap around guava utilities
        // This implementation is just for "make it work"
        RangeSet<T> rangeSet = TreeRangeSet.create();
        FluentIterable.from(ranges)
                .append(others)
                .forEach(range -> rangeSet.add(toGuavaRange(range)));

        return ImmutableList.copyOf(TiFluentIterable.from(rangeSet.asRanges())
                .transform(TiRange::toTiRange));
    }

    public static <T extends Comparable<T>> Range<T> toGuavaRange(TiRange<T> range) {
        return Range.range(range.getLowValue(),
                range.isLeftOpen() ? BoundType.OPEN : BoundType.CLOSED,
                range.getHighValue(),
                range.isRightOpen() ? BoundType.OPEN : BoundType.CLOSED);
    }

    public static <T extends Comparable<T>> TiRange<T> toTiRange(Range<T> range) {
        return TiRange.create(range.lowerEndpoint(),
                              range.upperEndpoint(),
                              range.lowerBoundType().equals(BoundType.OPEN),
                              range.upperBoundType().equals(BoundType.OPEN),
                              Comparator.naturalOrder());
    }

    public E getLowValue() {
        return lowVal;
    }

    public E getHighValue() {
        return highVal;
    }

    public boolean contains(E v) {
        return compare(v) == 0;
    }

    public boolean isLeftOpen() {
        return leftOpen;
    }

    public boolean isRightOpen() {
        return rightOpen;
    }

    public Comparator<E> getComparator() {
        return comparator;
    }

    public int compare(E v) {
        if (!isLeftOpen()  && comparator.compare(getLowValue(), v) == 0 ||
            !isRightOpen() && comparator.compare(getHighValue(), v) == 0) {
            return 0;
        }
        if (comparator.compare(getLowValue(), v) >= 0) {
            return -1;
        }
        if (comparator.compare(getHighValue(), v) <= 0) {
            return 1;
        }
        return 0;
    }

    public static final TiRange<Long> FULL_LONG_RANGE = TiRange.create(Long.MIN_VALUE, Long.MAX_VALUE);

    @Override
    public String toString() {
        String lowStr = getLowValue().toString();
        String highStr = getHighValue().toString();
        if (lowVal.getClass().equals(ByteString.class)) {
            lowStr = ((ByteString)lowVal).toStringUtf8();
            highStr = ((ByteString)highVal).toStringUtf8();
        }
        return String.format("%s%s,%s%s", isLeftOpen()? "(" : "[",
                                          lowStr, highStr,
                                          isRightOpen() ? ")" : "]");
    }
}
