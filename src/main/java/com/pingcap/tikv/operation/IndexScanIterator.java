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
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.RangeSplitter;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;
import gnu.trove.list.array.TLongArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class IndexScanIterator implements Iterator<Row> {
  private final Iterator<Long> handleIterator;
  private final TiSelectRequest selReq;
  private final Snapshot snapshot;
  private Iterator<Row> rowIterator;
  final ExecutorService pool = Executors.newFixedThreadPool(10);
  final ExecutorCompletionService<Iterator<Row>> completionService = new ExecutorCompletionService<>(pool);

  private final int MAX_BATCH = 10000 * 1000;
  private int batchCount = 0;
  public static boolean old = true;

  public IndexScanIterator(Snapshot snapshot, TiSelectRequest req, Iterator<Long> handleIterator) {
    this.selReq = req;
    this.handleIterator = handleIterator;
    this.snapshot = snapshot;
  }

  private TLongArrayList feedBatch() {
    TLongArrayList handles = new TLongArrayList(MAX_BATCH);
    while (handleIterator.hasNext()) {
      handles.add(handleIterator.next());
      if (handles.size() == MAX_BATCH) {
        break;
      }
    }
    return handles;
  }

  public boolean hasNextOld() {
    if (rowIterator == null) {
      TLongArrayList handles = new TLongArrayList(4096);
      while (handleIterator.hasNext()) {
        handles.add(handleIterator.next());
      }
      TiSession session = snapshot.getSession();
      RangeSplitter splitter = RangeSplitter.newSplitter(session.getRegionManager());

      rowIterator = SelectIterator.getRowIterator(
          selReq,
          splitter.splitHandlesByRegion(selReq.getTableInfo().getId(), handles),
          snapshot.getSession());
    }
    return rowIterator.hasNext();
  }

  @Override
  public boolean hasNext() {
    if (old) {
      return hasNextOld();
    }
    try {
      if (rowIterator == null) {
        TiSession session = snapshot.getSession();
        while (handleIterator.hasNext()) {
          TLongArrayList handles = feedBatch();
          batchCount++;
          completionService.submit(() -> {
            List<RegionTask> tasks = RangeSplitter
                .newSplitter(session.getRegionManager())
                .splitHandlesByRegion(selReq.getTableInfo().getId(), handles);
            return SelectIterator.getRowIterator(selReq, tasks, session);
          });
        }
        rowIterator = completionService.take().get();
        batchCount--;
      }

      if (rowIterator.hasNext()) {
        return true;
      }

      if (batchCount == 0) {
        return false;
      }
      rowIterator = completionService.take().get();
      batchCount--;
    } catch (Exception e) {
      throw new TiClientInternalException("Error reading rows from handle", e);
    }
    return rowIterator.hasNext();
  }

  @Override
  public Row next() {
    if (hasNext()) {
      return rowIterator.next();
    } else {
      throw new NoSuchElementException();
    }
  }
}
