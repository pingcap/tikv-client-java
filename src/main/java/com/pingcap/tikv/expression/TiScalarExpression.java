package com.pingcap.tikv.expression;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.type.FieldType;
import com.pingcap.tikv.util.TiFluentIterable;

import java.util.List;
import java.util.Map;

public class TiScalarExpression extends TiExpr {
    public enum Op {
        LT, LE, EQ, NE, GE, GT,
        NullEQ, In, Like,
        Plus, Minus, Mul, Div, Mod, IntDiv,
        And, Or, Not, LogicXor,
        BitAnd, BitOr, Xor, LeftShift, RightShift,
        Case, Coalesce, If, IfNull, IsNull, NullIf
    }

    static final Map<Op, ExprType> opCodeMap = ImmutableMap.<Op, ExprType>builder()
            .put(Op.LT, ExprType.LT)
            .put(Op.LE, ExprType.LE)
            .put(Op.EQ, ExprType.EQ)
            .put(Op.NE, ExprType.GE)
            .put(Op.GE, ExprType.GE)
            .put(Op.GT, ExprType.GT)
            .put(Op.NullEQ, ExprType.NullEQ)
            .put(Op.In, ExprType.In)
            .put(Op.Like, ExprType.Like)
            .put(Op.Plus, ExprType.Plus)
            .put(Op.Minus, ExprType.Minus)
            .put(Op.Mul, ExprType.Mul)
            .put(Op.Div, ExprType.Div)
            .put(Op.Mod, ExprType.Mod)
            .put(Op.IntDiv, ExprType.IntDiv)
            .put(Op.And, ExprType.And)
            .put(Op.Or, ExprType.Or)
            .put(Op.Not, ExprType.Not)
            .put(Op.LogicXor, ExprType.Xor)
            .put(Op.BitAnd, ExprType.BitAnd)
            .put(Op.BitOr, ExprType.BitOr)
            .put(Op.Xor, ExprType.BitXor)
            .put(Op.LeftShift, ExprType.LeftShift)
            .put(Op.RightShift, ExprType.RighShift)
            .put(Op.Case, ExprType.Case)
            .put(Op.Coalesce, ExprType.Coalesce)
            .put(Op.If, ExprType.If)
            .put(Op.IfNull, ExprType.IfNull)
            .put(Op.IsNull, ExprType.IsNull)
            .put(Op.NullIf, ExprType.NullIf)
            .build();

    private Op opCode;
    private List<TiExpr> args;

    public static TiScalarExpression create(Op opCode, TiExpr... args) {
        return new TiScalarExpression(opCode, args);
    }

    private TiScalarExpression(Op opCode, TiExpr... args) {
        this.opCode = opCode;
        this.args = ImmutableList.copyOf(args);
    }

    @Override
    public Expr toProto() {
        Expr.Builder builder = Expr.newBuilder();
        ExprType type = opCodeMap.get(opCode);
        if (type == null) {
            throw new UnsupportedOperationException("OpCode " + opCode.name() + " is not supported");
        }
        builder.setTp(type);
        builder.addAllChildren(TiFluentIterable
                .from(args)
                .transform(arg -> arg.toProto())
        );

        return builder.build();
    }

    @Override
    public FieldType getType() {
        return null;
    }
}
