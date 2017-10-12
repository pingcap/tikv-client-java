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

package com.pingcap.tikv.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.TableCodec;
import com.pingcap.tikv.kvproto.Coprocessor;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiIndexColumn;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import java.util.List;

public class KeyRangeUtils {
  public static Range toRange(Coprocessor.KeyRange range) {
    if (range == null || (range.getStart().isEmpty() && range.getEnd().isEmpty())) {
      return Range.all();
    }
    if (range.getStart().isEmpty()) {
      return Range.lessThan(Comparables.wrap(range.getEnd()));
    }
    if (range.getEnd().isEmpty()) {
      return Range.atLeast(Comparables.wrap(range.getStart()));
    }
    return Range.closedOpen(Comparables.wrap(range.getStart()), Comparables.wrap(range.getEnd()));
  }

  public static String toString(Coprocessor.KeyRange range) {
    return String.format("[%s, %s]",
        TableCodec.decodeRowKey(range.getStart()),
        TableCodec.decodeRowKey(range.getEnd()));
  }

  public static String toString(Coprocessor.KeyRange range, List<DataType> types) {
    return String.format("[%s, %s]",
        TableCodec.decodeIndexSeekKey(range.getStart(), types),
        TableCodec.decodeIndexSeekKey(range.getEnd(), types));
  }

  public static List<DataType> getIndexColumnTypes(TiTableInfo table, TiIndexInfo index) {
    ImmutableList.Builder<DataType> types = ImmutableList.builder();
    for (TiIndexColumn indexColumn : index.getIndexColumns()) {
      TiColumnInfo tableColumn = table.getColumns().get(indexColumn.getOffset());
      types.add(tableColumn.getType());
    }
    return types.build();
  }

  public static Range makeRange(ByteString startKey, ByteString endKey) {
    if (startKey.isEmpty() && endKey.isEmpty()) {
      return Range.all();
    }
    if (startKey.isEmpty()) {
      return Range.lessThan(Comparables.wrap(endKey));
    } else if (endKey.isEmpty()) {
      return Range.atLeast(Comparables.wrap(startKey));
    }
    return Range.closedOpen(Comparables.wrap(startKey), Comparables.wrap(endKey));
  }

  public static Coprocessor.KeyRange makeCoprocRangeWithHandle(long tableId, long startHandle, long endHandle) {
    ByteString startKey = TableCodec.encodeRowKeyWithHandle(tableId, startHandle);
    ByteString endKey = TableCodec.encodeRowKeyWithHandle(tableId, endHandle);
    return KeyRange.newBuilder().setStart(startKey).setEnd(endKey).build();
  }

  public static String formatByteString(ByteString key) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < key.size(); i++) {
      sb.append(key.byteAt(i) & 0xff);
      if (i < key.size() - 1) {
        sb.append(",");
      }
    }
    return sb.toString();
  }
}
