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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.Snapshot;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.grpc.Coprocessor;
import com.pingcap.tikv.grpc.Coprocessor.KeyRange;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.util.Comparables;

import java.util.*;
import java.util.stream.Collectors;

// A very bad implementation of Index Scanner barely made work
// TODO: need to make it parallel and group indexes
public class IndexScanIterator implements Iterator<Row> {
  private final Iterator<Row> iter;
  private final TiSelectRequest selReq;
  private final Snapshot snapshot;
  private final boolean singleRead;

  public IndexScanIterator(
      Snapshot snapshot, TiSelectRequest req, Iterator<Row> iter, boolean singleRead) {
    this.iter = iter;
    this.selReq = req;
    this.snapshot = snapshot;
    this.singleRead = singleRead;
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  private List<KeyRange> mergeKeyRangeList(List<KeyRange> keyRangeList) {
    // sort key ranges according to its start key in order to do further merge.
    List<KeyRange> newKeyRanges = new LinkedList<>();
    keyRangeList.sort((a, b) -> Comparables.wrap(a.getStart()).compareTo(b.getStart()));
    for(int i = 0; i < keyRangeList.size() - 1; i++) {
        KeyRange a = keyRangeList.get(i);
        KeyRange b = keyRangeList.get(i + 1);
        // since keyRangeList is already sorted. when a's end is larger or equal to b's start
        // a and b are two keys can be merged since a's start is always smaller or equal to b's start.
        // e.g.
        // [1, 2) and [2, 3)
        // [1, 4) and [2, 3)
        if(Comparables.wrap(a.getEnd()).compareTo(b.getStart()) >= 0) {
          ByteString largerEnd;
          if (Comparables.wrap(a.getEnd()).compareTo(b.getEnd()) >= 0) {
            largerEnd = a.getEnd();
          } else {
            largerEnd = b.getEnd();
          }
          newKeyRanges.add(KeyRange.newBuilder().setStart(a.getStart()).setEnd(largerEnd).build());
        }
    }
    return newKeyRanges;
  }

  private Iterator<Row> doubleRead() {
    // first read of double read's result are handles.
    // This is actually keys of our second read.
    List<KeyRange> keyRangeList = new LinkedList<>();
    while(iter.hasNext()) {
      Row r = iter.next();
      long handle = r.getLong(0);
      // in order to improve the speed of second read, we merge key if necessary.
      // [1, 3), [3, 4) will be [1, 4) after merge
      // [1, 2), [3, 4) will remain as same since they do not share any intersection.
      ByteString startKey = TableCodec.encodeRowKeyWithHandle(selReq.getTableInfo().getId(), handle);
      ByteString endKey = ByteString.copyFrom(KeyUtils.prefixNext(startKey.toByteArray()));
      keyRangeList.add(KeyRange.newBuilder().setStart(startKey).setEnd(endKey).build());
    }

    selReq.resetRanges(mergeKeyRangeList(keyRangeList));
    return snapshot.select(selReq);
  }

  private Iterator<Row> singleRead() {
    return iter;
  }

  @Override
  public Row next() {
    if (!singleRead) {
      return doubleRead().next();
    }
    return singleRead().next();
  }
}
