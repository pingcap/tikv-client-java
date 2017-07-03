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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.RegionManager;
import com.pingcap.tikv.RegionStoreClient;
import com.pingcap.tikv.TiRegion;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.grpc.Coprocessor.KeyRange;
import com.pingcap.tikv.grpc.Metapb.Region;
import com.pingcap.tikv.grpc.Metapb.Store;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.row.RowReader;
import com.pingcap.tikv.row.RowReaderFactory;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.Types;
import com.pingcap.tikv.util.RangeSplitter;
import com.pingcap.tikv.util.RangeSplitter.RegionTask;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class SelectIterator implements Iterator<Row> {
    protected final TiSession session;
    private final List<RegionTask> rangeToRegions;


    private ChunkIterator chunkIterator;
    protected int index = 0;
    private boolean eof = false;
    private Function<List<RegionTask>, Boolean> readNextRegionFn;
    private SchemaInfer schemaInfer;
    private final boolean indexScan;
    private static final DataType[] handleTypes = new DataType[]{DataTypeFactory.of(Types.TYPE_LONG)};

    @VisibleForTesting
    public SelectIterator(List<Chunk> chunks, TiSelectRequest req) {
        this.session = null;
        this.schemaInfer = SchemaInfer.create(req);
        this.rangeToRegions = null;
        this.readNextRegionFn = rangeToRegions -> {
            chunkIterator = new ChunkIterator(chunks);
            return true;
        };
        indexScan = false;
    }

    public SelectIterator(TiSelectRequest req,
                          List<RegionTask> rangeToRegionsIn,
                          TiSession session,
                          RegionManager regionManager,
                          boolean indexScan) {
        this.rangeToRegions = rangeToRegionsIn;
        this.session = session;
        this.schemaInfer = SchemaInfer.create(req);
        this.indexScan = indexScan;
        this.readNextRegionFn = (rangeToRegions) -> {
            if (eof || index >= rangeToRegions.size()) {
                return false;
            }

            RegionTask regionTask = rangeToRegions.get(index++);

            List<KeyRange> ranges = regionTask.getRanges();
            TiRegion region = regionTask.getRegion();
            Store store = regionTask.getStore();

            try (RegionStoreClient client = RegionStoreClient.create(region, store, session, regionManager)) {
                SelectResponse resp =
                        client.coprocess(indexScan ? req.buildAsIndexScan() : req.build(), ranges);
                if (resp == null) {
                    eof = true;
                    return false;
                }
                chunkIterator = new ChunkIterator(resp.getChunksList());
            } catch (Exception e) {
                eof = true;
                throw new TiClientInternalException("Error Closing Store client.", e);
            }
            return true;
        };
    }

    public SelectIterator(TiSelectRequest req,
                          TiSession session,
                          RegionManager rm,
                          boolean indexScan) {
        this(req, RangeSplitter.newSplitter(rm).splitRangeByRegion(req.getRanges()),
                                                                   session,
                                                                   rm,
                                                                   indexScan
        );
    }

    private boolean readNextRegion() {
        return this.readNextRegionFn.apply(rangeToRegions);
    }

    @Override
    public boolean hasNext() {
        if (eof) return false;
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
            RowReader reader = RowReaderFactory
                    .createRowReader(new CodecDataInput(rowData));
            // TODO: Make sure if only handle returned
            if (indexScan) {
                return reader.readRow(handleTypes);
            } else {
                return reader.readRow(this.schemaInfer.getTypes().toArray(new DataType[0]));
            }
        } else {
            throw new NoSuchElementException();
        }
    }

    private static class ChunkIterator implements Iterator<ByteString> {
        private final List<Chunk> chunks;
        private int chunkIndex;
        private int metaIndex;
        private int bufOffset;
        private boolean eof;

        ChunkIterator(List<Chunk> chunks) {
            // Read and then advance semantics
            this.chunks = chunks;
            chunkIndex = 0;
            metaIndex = 0;
            bufOffset = 0;
            if (chunks.size() == 0 ||
                    chunks.get(0).getRowsMetaCount() == 0 ||
                    chunks.get(0).getRowsData().size() == 0) {
                eof = true;
            }
        }

        @Override
        public boolean hasNext() {
            return !eof;
        }

        private void advance() {
            if (eof) return;
            Chunk c = chunks.get(chunkIndex);
            bufOffset += c.getRowsMeta(metaIndex++).getLength();
            if (metaIndex >= c.getRowsMetaCount()) {
                // seek for next non-empty chunk
                while (++chunkIndex < chunks.size() &&
                        chunks.get(chunkIndex).getRowsMetaCount() == 0) {
                    ;
                }
                if (chunkIndex >= chunks.size()) {
                    eof = true;
                    return;
                }
                metaIndex = 0;
                bufOffset = 0;
            }
        }

        @Override
        public ByteString next() {
            Chunk c = chunks.get(chunkIndex);
            long endOffset = c.getRowsMeta(metaIndex).getLength() + bufOffset;
            if (endOffset > Integer.MAX_VALUE) {
                throw new TiClientInternalException("Offset exceeded MAX_INT.");
            }
            ByteString rowData = c.getRowsData();
            ByteString result = rowData.substring(bufOffset, (int) endOffset);
            advance();
            return result;
        }
    }
}
