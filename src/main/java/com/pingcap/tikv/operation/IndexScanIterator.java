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

import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.KeyRangeUtils;
import com.pingcap.tikv.util.RangeSplitter;
import gnu.trove.list.array.TLongArrayList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class IndexScanIterator implements Iterator<Row> {
  private final Iterator<Long> handleIterator;
  private final TiSelectRequest selReq;
  private final Snapshot snapshot;
  private Iterator<Row> rowIterator;

  public IndexScanIterator(Snapshot snapshot, TiSelectRequest req, Iterator<Long> handleIterator) {
    this.selReq = req;
    this.handleIterator = handleIterator;
    this.snapshot = snapshot;
  }

  private List<KeyRange> mergeKeyRangeList(TLongArrayList handles) {
    List<KeyRange> newKeyRanges = new ArrayList<>(handles.size());
    // guard. only allow handles size larger than 1 pursues further.
    if (handles.size() == 0) {
      return newKeyRanges;
    }

    // sort handles first
    handles.sort();
    // merge all discrete key ranges.
    // e.g.
    // original key range is 1 as [1, 2), 2 as [2, 3), 3 as [3, 4)
    // after merge, the result is [1, 4)
    // original key range is 1 as [1, 2), 3 as [3, 4), 4 as [4, 5)
    // after merge, the result is [1, 2), [3, 5)
    long startHandle = handles.get(0);
    long endHandle = startHandle;
    for (int i = 1; i < handles.size(); i++) {
      long curHandle = handles.get(i);
      if (endHandle + 1 == curHandle) {
        endHandle = curHandle;
        continue;
      } else {
        newKeyRanges.add(KeyRangeUtils.makeCoprocRangeWithHandle(selReq.getTableInfo().getId(),
                                                                 startHandle,
                                                                 endHandle + 1));
        startHandle = curHandle;
        endHandle = startHandle;
      }
    }
    newKeyRanges.add(KeyRangeUtils.makeCoprocRangeWithHandle(selReq.getTableInfo().getId(), startHandle, endHandle + 1));
    return newKeyRanges;
  }

  @Override
  public boolean hasNext() {
    if (rowIterator == null) {
      TLongArrayList handles = new TLongArrayList(4096);
      while (handleIterator.hasNext()) {
        handles.add(handleIterator.next());
      }
      selReq.resetRanges(mergeKeyRangeList(handles));
      TiSession session = snapshot.getSession();
      rowIterator = SelectIterator.getRowIterator(
          selReq,
          RangeSplitter.newSplitter(session.getRegionManager()).splitRangeByRegion(selReq.getRanges()),
          snapshot.getSession());
    }
    return rowIterator.hasNext();
  }

  @Override
  public Row next() {
    return rowIterator.next();
  }
}
