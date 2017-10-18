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
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.RangeSplitter;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;
import gnu.trove.list.array.TLongArrayList;
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

  @Override
  public boolean hasNext() {
    if (rowIterator == null) {
      TLongArrayList handles = new TLongArrayList(4096);
      while (handleIterator.hasNext()) {
        handles.add(handleIterator.next());
      }
      TiSession session = snapshot.getSession();
      List<RegionTask> tasks = RangeSplitter
          .newSplitter(session.getRegionManager())
          .splitHandlesByRegion(selReq.getTableInfo().getId(), handles);

      rowIterator = SelectIterator.getRowIterator(selReq, tasks, session);
    }
    return rowIterator.hasNext();
  }

  @Override
  public Row next() {
    return rowIterator.next();
  }
}
