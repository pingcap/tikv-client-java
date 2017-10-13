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
import com.google.protobuf.InvalidProtocolBufferException;
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.exception.SelectException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.kvproto.Coprocessor;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import com.pingcap.tikv.kvproto.Metapb.Store;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.region.RegionManager;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.row.RowReader;
import com.pingcap.tikv.row.RowReaderFactory;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.Types;
import com.pingcap.tikv.util.RangeSplitter;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public class SelectIterator implements Iterator<Row> {

  protected final TiSession session;
  private final List<RegionTask> regionTasks;

  private ChunkIterator chunkIterator;
  protected int index = 0;
  private boolean eof = false;
  private SchemaInfer schemaInfer;
  private final boolean indexScan;
  private TiSelectRequest tiReq;
  private RegionManager regionManager;
  private static final DataType[] handleTypes =
      new DataType[]{DataTypeFactory.of(Types.TYPE_LONG)};

  public SelectIterator(
      TiSelectRequest req,
      List<RegionTask> regionTasks,
      TiSession session,
      boolean indexScan) {
    this.regionTasks = regionTasks;
    this.tiReq = req;
    this.session = session;
    this.schemaInfer = SchemaInfer.create(req);
    this.indexScan = indexScan;
  }

  public SelectIterator(
      TiSelectRequest req,
      RegionManager rm,
      TiSession session,
      boolean indexScan) {
    this.regionTasks = RangeSplitter.newSplitter(rm).splitRangeByRegion(req.getRanges());
    this.tiReq = req;
    this.session = session;
    this.regionManager = rm;
    this.schemaInfer = SchemaInfer.create(req);
    this.indexScan = indexScan;
  }

  public SelectIterator(TiSelectRequest req, TiSession session, RegionManager rm,
      boolean indexScan) {
    this(req, rm, session, indexScan);
  }

  private void handleOnRegionSplit(RegionTask regionTask, TiSelectRequest req, List<Chunk> chunks) {
    List<RegionTask> regionTasks = RangeSplitter.newSplitter(this.regionManager).splitRangeByRegion(regionTask.getRanges());
    for(RegionTask t : regionTasks) {
      List<Chunk> resFromCurTask = createClientAndSendReq(t, req, chunks);
      if(resFromCurTask != null) {
        chunks.addAll(resFromCurTask);
      }
    }
  }

  private List<Chunk> createClientAndSendReq(RegionTask regionTask,
      TiSelectRequest req, List<Chunk> chunks) {
    List<KeyRange> ranges = regionTask.getRanges();
    TiRegion region = regionTask.getRegion();
    Store store = regionTask.getStore();

    RegionStoreClient client;
    try {
      client = RegionStoreClient.create(region, store, session);
      Coprocessor.Response resp = client.coprocess(req.buildScan(indexScan), ranges);
      // if resp is null, then indicates eof.
      if (resp == null) {
        eof = true;
        return null;
      }
      if(resp.hasRegionError()) {
        handleOnRegionSplit(regionTask, req, chunks);
      }
      chunks.addAll(coprocessorHelper(resp).getChunksList());
      return chunks;
    } catch (Exception e) {
      throw new TiClientInternalException("Error Closing Store client.", e);
    }
  }

  public static SelectResponse coprocessorHelper(Coprocessor.Response resp) {
    try {
      SelectResponse selectResp = SelectResponse.parseFrom(resp.getData());
      if (selectResp.hasError()) {
        throw new SelectException(selectResp.getError(), selectResp.getError().getMsg());
      }
      return selectResp;
    } catch (InvalidProtocolBufferException e) {
      throw new TiClientInternalException("Error parsing protobuf for coprocessor response.", e);
    }
  }

  private boolean readNextRegion() {
    if (eof || index >= regionTasks.size()) {
      return false;
    }

    RegionTask regionTask = regionTasks.get(index++);
    List<Chunk> chunks = new ArrayList<>();
    createClientAndSendReq(regionTask, this.tiReq, chunks);
    if (chunks.isEmpty()) {
      return false;
    }
    chunkIterator = new ChunkIterator(chunks);
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

  @Override
  public Row next() {
    if (hasNext()) {
      ByteString rowData = chunkIterator.next();
      RowReader reader = RowReaderFactory.createRowReader(new CodecDataInput(rowData));
      if (indexScan) {
        return reader.readRow(handleTypes);
      } else {
        return reader.readRow(this.schemaInfer.getTypes().toArray(new DataType[0]));
      }
    } else {
      throw new NoSuchElementException();
    }
  }
}
