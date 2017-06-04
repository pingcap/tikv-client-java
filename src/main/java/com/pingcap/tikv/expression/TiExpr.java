package com.pingcap.tikv.expression;

import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.LongUtils;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.type.FieldType;

import java.util.List;

public interface TiExpr {
    Expr toProto();
    FieldType getType();
}
