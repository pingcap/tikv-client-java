package com.pingcap.tikv.meta;

import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.google.protobuf.ByteString;
import com.google.common.primitives.Longs;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.LongUtils;

import java.util.List;

public class TiExpr {
  private TiTableInfo table;
  private ExprType exprType;
  private static List<TiExpr> childrens;

  public static TiExpr create(ExprType exprType, String value) {
    return new TiExpr(exprType, value);
  }

  private TiExpr(ExprType exprType, String value) {
    this.exprType = exprType;
  }

  private boolean isExprTypeSupported(ExprType exprType) {
    return true;
  }

  public TiExpr addChildren(TiExpr expr) {
    if (isExprTypeSupported(expr.getExprType())) {
      // TODO throw a exception here
      // throw new Exception();
    }
    this.childrens.add(expr);
    return this;
  }

  public Expr toProto() {
    // TODO this is only for Sum
    Expr.Builder builder = Expr.newBuilder();
    Expr.Builder columnRefBuilder = Expr.newBuilder();
    builder.setTp(exprType);
    columnRefBuilder.setTp(ExprType.ColumnRef);
    CodecDataOutput cdo = new CodecDataOutput();
    //TODO: tableinfo passed into and getColumnId
    LongUtils.writeLong(cdo, 1);
    columnRefBuilder.setVal(ByteString.copyFrom(cdo.toBytes()));
    builder.addChildren(0, columnRefBuilder.build());
    return builder.build();
  }

  public ExprType getExprType() {
    return this.exprType;
  }
}
