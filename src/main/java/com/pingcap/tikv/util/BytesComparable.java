/*
 *
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
 *
 */

package com.pingcap.tikv.util;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

public class BytesComparable implements Comparable<BytesComparable>, Serializable {
  private final byte[] value;
  private final Object obj;

  public static BytesComparable wrap(Object obj) {
    return new BytesComparable(obj);
  }

  private final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
  private BytesComparable(Object obj) {
    if(obj instanceof BytesComparable) {
      throw new IllegalStateException("This object is already comparable");
    }
    this.value = convertObjectToBytes(obj);
    this.obj = obj;
  }

  private byte[] convertObjectToBytes(Object o) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutput out;
    byte[] objBytes = new byte[0];
    try {
      out = new ObjectOutputStream(bos);
      out.writeObject(o);
      out.flush();
      objBytes = bos.toByteArray();
    } catch (IOException ignored) {
    }
    return objBytes;
  }

  private Object convertBytesToObject(byte[] bytes) {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    Object o = null;
    ObjectInput in;
    try {
      in = new ObjectInputStream(bis);
      o = in.readObject();
    } catch (IOException | ClassNotFoundException ignored) {
    }
    return o;
  }

  @Override
  public int compareTo(BytesComparable o) {
    if(getObjectValue() instanceof ByteString) {
      ByteString bytes = (ByteString) getObjectValue();
      ByteString otherBytes = (ByteString) o.getObjectValue();
      int n = Math.min(bytes.size(), otherBytes.size());
      for (int i = 0, j = 0; i < n; i++, j++) {
        int cmp = UnsignedBytes.compare(bytes.byteAt(i), otherBytes.byteAt(j));
        if (cmp != 0) return cmp;
      }
      // one is the prefix of other then the longer is larger
      return bytes.size() - otherBytes.size();
    }
    return comparator.compare(this.value, o.getValue());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(value);
  }

  @Override
  public boolean equals(Object o) {
    if(o == this) return true;
    if(o instanceof BytesComparable)
      return compareTo(BytesComparable.wrap(o)) == 0;
    return false;
  }

  public byte[] getValue() {
    return value;
  }

  public Object getObjectValue() {
    return obj;
  }
}
