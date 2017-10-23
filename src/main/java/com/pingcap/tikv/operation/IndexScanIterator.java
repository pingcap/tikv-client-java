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
import com.pingcap.tikv.TiConfiguration;
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
  private final ExecutorService pool;
  private final ExecutorCompletionService<Iterator<Row>> completionService;

  private int batchCount = 0;
  private final int batchSize;

  public IndexScanIterator(Snapshot snapshot, TiSelectRequest req, Iterator<Long> handleIterator) {
    TiConfiguration conf = snapshot.getSession().getConf();
    this.selReq = req;
    this.handleIterator = handleIterator;
    this.snapshot = snapshot;
    this.batchSize = conf.getIndexScanBatchSize();
    this.pool = Executors.newFixedThreadPool(conf.getIndexScanConcurrency());
    this.completionService = new ExecutorCompletionService(pool);
  }

  private TLongArrayList feedBatch() {
    TLongArrayList handles = new TLongArrayList(1000000);
    while (handleIterator.hasNext()) {
      handles.add(handleIterator.next());
      if (batchSize <= handles.size()) {
        break;
      }
    }
    return handles;
  }

  @Override
  public boolean hasNext() {
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
