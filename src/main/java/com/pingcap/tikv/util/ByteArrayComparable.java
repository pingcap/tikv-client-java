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

import static com.pingcap.tikv.types.Types.TYPE_FLOAT;
import static com.pingcap.tikv.types.Types.TYPE_LONG;
import static com.pingcap.tikv.types.Types.TYPE_NEW_DECIMAL;
import static com.pingcap.tikv.types.Types.TYPE_VARCHAR;

import com.google.common.primitives.UnsignedBytes;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.DecimalType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.RealType;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;

public class ByteArrayComparable implements Comparable<ByteArrayComparable>, Serializable {
  private byte[] value;
  private ExprType exprType;
  private DataType dataType;
  private StringBuilder sb = new StringBuilder();

  private final Comparator<byte[]> comparator = UnsignedBytes.lexicographicalComparator();
  public ByteArrayComparable(byte[] value) {
    this.value = value;
  }

  public ByteArrayComparable(ByteString value) {
    this.value = value.toByteArray();
  }

  public ByteArrayComparable(Number val) {
    CodecDataOutput cdo = new CodecDataOutput();
    IntegerType.writeLong(cdo, val.longValue());
    this.exprType = ExprType.Int64;
    this.dataType = DataTypeFactory.of(TYPE_LONG);
    this.value = cdo.toBytes();
    sb.append(val);
  }

  public ByteArrayComparable(String val) {
    this.exprType = ExprType.String;
    this.dataType = DataTypeFactory.of(TYPE_VARCHAR);
    this.value = val.getBytes();
    sb.append(val);
  }

  public ByteArrayComparable(Float val) {
    this.exprType = ExprType.Float32;
    CodecDataOutput cdo = new CodecDataOutput();
    RealType.writeFloat(cdo, val);
    this.dataType = DataTypeFactory.of(TYPE_FLOAT);
    this.value = cdo.toBytes();
    sb.append(val);
  }

  public ByteArrayComparable(BigDecimal val) {
    this.exprType = ExprType.MysqlDecimal;
    CodecDataOutput cdo = new CodecDataOutput();
    DecimalType.writeDecimal(cdo, val);
    // Why does not this have data type.
    this.value = cdo.toBytes();
    sb.append(val);
  }

  public ByteArrayComparable(Double val) {
    this.exprType = ExprType.Float64;
    CodecDataOutput cdo = new CodecDataOutput();
    RealType.writeDouble(cdo, val);
    this.dataType = DataTypeFactory.of(TYPE_NEW_DECIMAL);
    this.value = cdo.toBytes();
    sb.append(val);
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public int compareTo(ByteArrayComparable o) {
    return comparator.compare(this.value, o.getValue());
  }

  public DataType getDataType() {
    return dataType;
  }

  public ExprType getExprType() {
    return exprType;
  }

  @Override
  public String toString() {
    return sb.toString();
  }


  public ByteString getByteString() {
    return ByteString.copyFrom(value);
  }

  @Override
  public boolean equals(Object other) {
    return true;
  }

  @Override
    public int hashCode() {
      return Arrays.hashCode(value);
    }
}
