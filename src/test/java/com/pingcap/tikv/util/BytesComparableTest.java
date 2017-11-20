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

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.Chunk;
import com.pingcap.tidb.tipb.SelectResponse;
import com.pingcap.tikv.RegionStoreClientTest;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.Test;

public class BytesComparableTest {
  @Test
  public void wrapTest() throws Exception {
    // compared as unsigned
    testBytes(new byte[] {1, 2, -1, 10}, new byte[] {1, 2, 0, 10}, x -> x > 0);
    testBytes(new byte[] {1, 2, 0, 10}, new byte[] {1, 2, 0, 10}, x -> x == 0);
    testBytes(new byte[] {1, 2, 0, 10}, new byte[] {1, 2, 1, 10}, x -> x < 0);
    testBytes(new byte[] {1, 2, 0, 10}, new byte[] {1, 2, 0}, x -> x > 0);
    String a = "key4";
    String b = "key6";
    testBytes(a.getBytes(), b.getBytes(), x -> x < 0);

    testComparable(1, 2, x -> x < 0);
    testComparable(13, 13, x -> x == 0);
    testComparable(13, 2, x -> x > 0);
  }

  private void testBytes(byte[] lhs, byte[] rhs, Function<Integer, Boolean> tester) {
    ByteString lhsBS = ByteString.copyFrom(lhs);
    ByteString rhsBS = ByteString.copyFrom(rhs);

    BytesComparable lhsComp = BytesComparable.wrap(lhsBS);
    BytesComparable rhsComp = BytesComparable.wrap(rhsBS);

    assertTrue(tester.apply(lhsComp.compareTo(rhsComp)));

    lhsComp = BytesComparable.wrap(lhs);
    rhsComp = BytesComparable.wrap(rhs);

    assertTrue(tester.apply(lhsComp.compareTo(rhsComp)));
  }

  private void testComparable(Object lhs, Object rhs, Function<Integer, Boolean> tester) {
    BytesComparable lhsComp = BytesComparable.wrap(lhs);
    BytesComparable rhsComp = BytesComparable.wrap(rhs);

    assertTrue(tester.apply(lhsComp.compareTo(rhsComp)));
  }

  TreeMap<BytesComparable, ByteString> dataMap = new TreeMap<>();

  @Test
  public void mapTest() {
    put("key1", "value1");
    put("key2", "value2");
    put("key4", "value4");
    put("key5", "value5");
    put("key6", "value6");
    put("key7", "value7");

   List<KeyRange> keyRanges =
        ImmutableList.of(
            RegionStoreClientTest.createByteStringRange(ByteString.copyFromUtf8("key1"), ByteString.copyFromUtf8("key4")),
            RegionStoreClientTest.createByteStringRange(
                ByteString.copyFromUtf8("key6"), ByteString.copyFromUtf8("key7")));

    SelectResponse.Builder builder = SelectResponse.newBuilder();
    for (KeyRange keyRange : keyRanges) {
      ByteString startKey = keyRange.getStart();
      SortedMap<BytesComparable, ByteString> kvs = dataMap.tailMap(BytesComparable.wrap(startKey));
      builder.addAllChunks(kvs.entrySet()
          .stream()
          .filter(Objects::nonNull)
          .filter(kv -> kv.getKey().compareTo(BytesComparable.wrap(keyRange.getEnd())) <= 0)
          .map(
              kv ->
                  Chunk.newBuilder()
                      .setRowsData(kv.getValue())
                      .build())
          .collect(Collectors.toList()));
    }
    assertEquals(5, builder.getChunksCount());
  }

  private void put(ByteString key, ByteString value) {
    dataMap.put(BytesComparable.wrap(key), value);
  }

  private void put(String key, String value) {
    put(ByteString.copyFromUtf8(key),
        ByteString.copyFromUtf8(value));
  }
}
