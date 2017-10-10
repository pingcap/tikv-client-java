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
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.SelectRequest;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.row.RowReader;
import com.pingcap.tikv.row.RowReaderFactory;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

public abstract class SelectIterator<T, RawT> implements Iterator<T> {

  protected final TiSession session;
  protected final List<RegionTask> regionTasks;

  protected ChunkIterator<RawT> chunkIterator;
  protected int index = 0;
  protected boolean eof = false;
  protected SchemaInfer schemaInfer;
  protected final Function<List<Chunk>, ChunkIterator<RawT>> chunkIteratorFactory;
  protected final SelectRequest request;

  public static SelectIterator<Row, ByteString> getRowIterator(TiSelectRequest req,
                                                   List<RegionTask> regionTasks,
                                                   TiSession session) {
    return new SelectIterator<Row, ByteString>(req.buildScan(false),
                                               regionTasks,
                                               session,
                                               SchemaInfer.create(req),
                                               (chunks) -> ChunkIterator.getRawBytesChunkIterator(chunks)) {
      @Override
      public Row next() {
        if (hasNext()) {
          ByteString rowData = chunkIterator.next();
          RowReader reader = RowReaderFactory.createRowReader(new CodecDataInput(rowData));
          return reader.readRow(this.schemaInfer.getTypes().toArray(new DataType[0]));
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  public static SelectIterator<Long, Long> getHandleIterator(TiSelectRequest req,
      List<RegionTask> regionTasks,
      TiSession session) {
    return new SelectIterator<Long, Long>(req.buildScan(true),
                                          regionTasks,
                                          session,
                                          SchemaInfer.create(req),
                                          (chunks) -> ChunkIterator.getHandleChunkIterator(chunks)) {
      @Override
      public Long next() {
        if (hasNext()) {
          return chunkIterator.next();
        } else {
          throw new NoSuchElementException();
        }
      }
    };
  }

  public SelectIterator(
      SelectRequest req,
      List<RegionTask> regionTasks,
      TiSession session,
      SchemaInfer infer,
      Function<List<Chunk>, ChunkIterator<RawT>> chunkIteratorFactory) {
    this.regionTasks = regionTasks;
    this.request = req;
    this.session = session;
    this.schemaInfer = infer;
    this.chunkIteratorFactory = chunkIteratorFactory;
  }

  private List<Chunk> createClientAndSendReq(RegionTask regionTask) {
    List<KeyRange> ranges = regionTask.getRanges();
    TiRegion region = regionTask.getRegion();
    Store store = regionTask.getStore();

    RegionStoreClient client;
    try {
      client = RegionStoreClient.create(region, store, session);
      SelectResponse resp = client.coprocess(request, ranges);
      // if resp is null, then indicates eof.
      if (resp == null) {
        eof = true;
        return null;
      }
      return resp.getChunksList();
    } catch (Exception e) {
      throw new TiClientInternalException("Error Closing Store client.", e);
    }
  }

  private boolean readNextRegion() {
    if (eof || index >= regionTasks.size()) {
      return false;
    }

    RegionTask regionTask = regionTasks.get(index++);
    List<Chunk> chunks = createClientAndSendReq(regionTask);
    if (chunks == null) {
      return false;
    }

    chunkIterator = chunkIteratorFactory.apply(chunks);
    return true;
  }

  @Override
  public boolean hasNext() {
    if (eof) {
      return false;
    }
    while (chunkIterator == null || !chunkIterator.hasNext()) {
      // Skip empty region until found one or EOF
      if (!readNextRegion()) {
        return false;
      }
    }
    return true;
  }
}
