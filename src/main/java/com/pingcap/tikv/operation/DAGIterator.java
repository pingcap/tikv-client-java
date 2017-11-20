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

import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.DAGRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.kvproto.Coprocessor;
import com.pingcap.tikv.kvproto.Metapb;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.row.RowReader;
import com.pingcap.tikv.row.RowReaderFactory;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.Types;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ExecutorCompletionService;

public abstract class DAGIterator<T> implements Iterator<T> {
  protected final TiSession session;
  private final List<RegionTask> regionTasks;
  private DAGRequest dagRequest;
  private static final DataType[] handleTypes =
      new DataType[]{DataTypeFactory.of(Types.TYPE_LONG)};
  protected final ExecutorCompletionService<Iterator<SelectResponse>> completionService;
  protected RowReader rowReader;
  private CodecDataInput dataInput;
  private boolean eof = false;
  private int taskIndex;
  private int chunkIndex;
  private List<Chunk> chunkList;
  protected SchemaInfer schemaInfer;
  protected Iterator<SelectResponse> responseIterator;

  private DAGIterator(DAGRequest req,
                      List<RegionTask> regionTasks,
                      TiSession session,
                      SchemaInfer infer) {
    this.dagRequest = req;
    this.session = session;
    this.regionTasks = regionTasks;
    this.schemaInfer = infer;
    this.completionService = new ExecutorCompletionService<>(session.getThreadPoolForTableScan());
    submitTasks();
  }

  private void submitTasks() {
    for (RegionTask task : regionTasks) {
      completionService.submit(() -> processByStreaming(task));
    }
  }

  public static DAGIterator<Row> getRowIterator(TiDAGRequest req,
                                                List<RegionTask> regionTasks,
                                                TiSession session) {
    return new DAGIterator<Row>(
        req.buildScan(false),
        regionTasks,
        session,
        SchemaInfer.create(req)
    ) {
      @Override
      public Row next() {
        if (hasNext()) {
          return rowReader.readRow(schemaInfer.getTypes().toArray(new DataType[0]));
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  public static DAGIterator<Long> getHandleIterator(TiDAGRequest req,
                                                    List<RegionTask> regionTasks,
                                                    TiSession session) {
    return new DAGIterator<Long>(
        req.buildScan(true),
        regionTasks,
        session,
        SchemaInfer.create(req)
    ) {
      @Override
      public Long next() {
        if (hasNext()) {
          return rowReader.readRow(handleTypes).getLong(0);
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  @Override
  public boolean hasNext() {
    if (eof) {
      return false;
    }

    while (chunkList == null ||
        chunkIndex >= chunkList.size() ||
        dataInput.available() <= 0
        ) {
      // First we check if our chunk list has remaining chunk
      if (tryAdvanceChunkIndex()) {
        createDataInputReader();
      }
      // If not, check next response/region
      else if (!advanceNextResponse() && !readNextRegionChunks()) {
        return false;
      }
    }

    return true;
  }


  private boolean tryAdvanceChunkIndex() {
    if (chunkList == null || chunkIndex >= chunkList.size() - 1) {
      return false;
    }

    chunkIndex++;
    return true;
  }

  private boolean readNextRegionChunks() {
    if (eof ||
        regionTasks == null ||
        taskIndex >= regionTasks.size()) {
      return false;
    }

    try {
      responseIterator = completionService.take().get();
      taskIndex++;
    } catch (Exception e) {
      throw new TiClientInternalException("Error reading region:", e);
    }

    return responseIterator != null && advanceNextResponse();
  }

  private void createDataInputReader() {
    Objects.requireNonNull(chunkList, "Chunk list should not be null.");
    if (0 > chunkIndex ||
        chunkIndex >= chunkList.size()) {
      throw new IllegalArgumentException();
    }
    dataInput = new CodecDataInput(chunkList.get(chunkIndex).getRowsData());
    rowReader = RowReaderFactory.createRowReader(dataInput);
  }

  private boolean hasMoreResponse() {
    return responseIterator != null && responseIterator.hasNext();
  }

  private boolean advanceNextResponse() {
    if (!hasMoreResponse()) return false;

    chunkList = responseIterator.next().getChunksList();
    if (null == chunkList || chunkList.isEmpty()) {
      return false;
    }
    chunkIndex = 0;
    createDataInputReader();
    return true;
  }

  private Iterator<SelectResponse> processByStreaming(RegionTask regionTask) {
    List<Coprocessor.KeyRange> ranges = regionTask.getRanges();
    TiRegion region = regionTask.getRegion();
    Metapb.Store store = regionTask.getStore();

    RegionStoreClient client;
    try {
      client = RegionStoreClient.create(region, store, session);
      Iterator<SelectResponse> responseIterator = client.coprocessStreaming(dagRequest, ranges);
      if (null == responseIterator) {
        eof = true;
        return null;
      }
      return responseIterator;
    } catch (Exception e) {
      throw new TiClientInternalException("Error Closing Store client.", e);
    }
  }
}
