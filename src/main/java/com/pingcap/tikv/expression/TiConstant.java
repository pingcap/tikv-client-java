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

package com.pingcap.tikv.expression;

import static com.pingcap.tikv.types.Types.*;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.*;
import com.pingcap.tikv.util.ByteArrayComparable;
import java.math.BigDecimal;
import java.util.Objects;

// TODO: This might need a refactor to accept an DataType?
public class TiConstant implements TiExpr {
  private ByteArrayComparable value;

  public TiConstant(Number value) {
    this.value = new ByteArrayComparable(value);
  }

  public TiConstant(Float value) {
    this.value = new ByteArrayComparable(value);
  }


  public TiConstant(Double value) {
    this.value = new ByteArrayComparable(value);
  }

  public TiConstant(BigDecimal value) {
    this.value = new ByteArrayComparable(value);
  }
  public TiConstant(String value) {
    this.value = new ByteArrayComparable(value);
  }

  public ByteArrayComparable getValue() {
    return value;
  }

  // refer to expr_to_pb.go:datumToPBExpr
  // But since it's a java client, we ignored
  // unsigned types for now
  // TODO: Add unsigned constant types support
  @Override
  public Expr toProto() {
    Expr.Builder builder = Expr.newBuilder();
    CodecDataOutput cdo = new CodecDataOutput();
    if(value == null) {
      builder.setTp(ExprType.Null);
      builder.setVal(cdo.toByteString());
      return builder.build();
    }
    builder.setTp(value.getExprType());
    cdo.write(value.getValue());
    builder.setVal(cdo.toByteString());
    return builder.build();
  }

  @Override
  public DataType getType() {
    if (value == null) {
      throw new TiExpressionException("NULL constant has no type");
    }
    return value.getDataType();
  }

  @Override
  public TiConstant bind(TiTableInfo table) {
    return this;
  }

  @Override
  public String toString() {
    return value.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof TiConstant) {
      return Objects.equals(value, ((TiConstant) other).value);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return value == null ? 0 : value.hashCode();
  }
}
