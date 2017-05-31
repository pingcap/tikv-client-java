package com.pingcap.tikv.expression;

import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.LongUtils;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.type.FieldType;

import java.util.List;

public abstract class TiExpr {
  protected TiTableInfo table;

  public abstract Expr toProto();
  public abstract FieldType getType();
}
