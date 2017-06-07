package com.pingcap.tikv.expression;


import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.BytesUtils;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.FloatUtils;
import com.pingcap.tikv.codec.LongUtils;
import com.pingcap.tikv.types.FieldType;
import com.pingcap.tikv.types.LongType;
import com.pingcap.tikv.types.VarCharType;

public class TiConstant implements TiExpr {
    private Object value;

    public static TiConstant create(Object value) {
        return new TiConstant(value);
    }

    private TiConstant(Object value) {
        this.value = value;
    }

    private boolean isIntegerType() {
        return value instanceof Long ||
                value instanceof Integer ||
                value instanceof Short ||
                value instanceof Byte;
    }

    @Override
    public Expr toProto() {
        Expr.Builder builder = Expr.newBuilder();
        CodecDataOutput cdo = new CodecDataOutput();
        // We don't allow build a unsigned long constant for now
        if (value == null) {
            builder.setTp(ExprType.Null);
        } else if (isIntegerType()) {
            builder.setTp(ExprType.Int64);
            LongUtils.writeLong(cdo, ((Number)value).longValue());
        } else if (value instanceof String) {
            builder.setTp(ExprType.String);
            BytesUtils.writeBytes(cdo, ((String)value).getBytes());
        } else if (value instanceof Float) {
            builder.setTp(ExprType.Float32);
            FloatUtils.writeFloat(cdo, (Float)value);
        } else if (value instanceof Double) {
            builder.setTp(ExprType.Float64);
            FloatUtils.writeDouble(cdo, (Double)value);
        } else {
            throw new TiExpressionException("Constant type not supported.");
        }
        builder.setVal(cdo.toByteString());

        return builder.build();
    }

    @Override
    public FieldType getType() {
        if (value == null) {
            throw new TiExpressionException("NULL constant has no type");
        } else if (isIntegerType()) {
            return LongType.DEF_SIGNED_TYPE;
        } else if (value instanceof String) {
            return VarCharType.DEF_TYPE;
        } else if (value instanceof Float) {
            throw new UnsupportedOperationException();
        } else if (value instanceof Double) {
            throw new UnsupportedOperationException();
        } else {
            throw new TiExpressionException("Constant type not supported.");
        }
    }
}
