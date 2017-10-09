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
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.row.Row;
import gnu.trove.list.array.TLongArrayList;
import java.util.*;


public class IndexScanIterator implements Iterator<Row> {
  private Iterator<Row> iter;
  private final TiSelectRequest selReq;
  private final Snapshot snapshot;

  public IndexScanIterator(Snapshot snapshot, TiSelectRequest req, Iterator<Row> iter, boolean singleRead) {
    this.iter = iter;
    this.selReq = req;
    this.snapshot = snapshot;
    if (singleRead) {
      this.iter = singleRead();
    } else {
      this.iter = doubleRead();
    }
  }

  private List<KeyRange> mergeKeyRangeList(TLongArrayList handles) {
    List<KeyRange> newKeyRanges = new LinkedList<>();
    // guard. only allow handles size larger than 2 pursues further.
    if (handles.size() < 2) {
      ByteString startKey = TableCodec.encodeRowKeyWithHandle(selReq.getTableInfo().getId(), handles.get(0));
      ByteString endKey = KeyUtils.getNextKeyInByteOrder(startKey);
      newKeyRanges.add(KeyRange.newBuilder().setStart(startKey).setEnd(endKey).build());
      return newKeyRanges;
    }
    // sort handles first
    handles.sort();
    // merge all discrete key ranges.
    // e.g.
    // original key range is [1, 2), [2, 3), [3, 4)
    // after merge, the result is [1, 4)
    // original key range is [1, 2), [3, 4), [4, 5)
    // after merge, the result is [1, 2), [3, 5)
    TLongArrayList startKeys = new TLongArrayList(64);
    for (int i = 0; i < handles.size() - 1; i++) {
      startKeys.add(handles.get(i));
      long nextStart = handles.get(i + 1);
      long end = handles.get(i) + 1;
      if (nextStart <= end) {
        continue;
      }

      ByteString startKey = TableCodec.encodeRowKeyWithHandle(selReq.getTableInfo().getId(), startKeys.get(0));
      ByteString endKey = TableCodec.encodeRowKeyWithHandle(selReq.getTableInfo().getId(), end);
      newKeyRanges.add(KeyRange.newBuilder().setStart(startKey).setEnd(endKey).build());
    }
    return newKeyRanges;
  }

  private Iterator<Row> doubleRead() {
    // first read of double read's result are handles.
    // This is actually keys of our second read.
    TLongArrayList handles = new TLongArrayList(64);
    while (iter.hasNext()) {
      Row r = iter.next();
      handles.add(r.getLong(0));
    }

    selReq.resetRanges(mergeKeyRangeList(handles));
    return snapshot.select(selReq);
  }

  private Iterator<Row> singleRead() {
    return iter;
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public Row next() {
    return iter.next();
  }
}
