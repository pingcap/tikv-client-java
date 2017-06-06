
package com.pingcap.tikv.operation;


import com.pingcap.tikv.expression.TiUnaryFunctionExpression;
import com.pingcap.tikv.expression.aggregate.*;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.type.FieldType;
import com.pingcap.tikv.type.StringType;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

public class SchemaInferer {

    public static TiFieldType create(TiSelectRequest tiSelectRequest) {
        return new TiFieldType(tiSelectRequest);
    }

    @Data
    public static class TiFieldType {
        private boolean isSingledGroup;
        private List<FieldType> fieldTypes;
        private TiSelectRequest tiSelectRequest;

        private TiFieldType(TiSelectRequest tiSelectRequest) {
            fieldTypes = new ArrayList<>();
            // Extract all column type information from TiExpr
            // Since fields are simple expression, intermediateFieldTypes shares the same
            // FieldType information.
            tiSelectRequest.getFields().forEach(
                    expr -> {
                        fieldTypes.add(expr.getType());
                    }
            );
            tiSelectRequest.getAggregates().forEach(
                    expr -> {
                        TiUnaryFunctionExpression fExpr = (TiUnaryFunctionExpression) expr;
                        switch (fExpr.getName()) {
                            case "sum":
                                Sum sExpr = (Sum)fExpr;
                                fieldTypes.add(sExpr.getType());
                                break;
                            case "count":
                                Count cExpr = (Count) fExpr;
                                fieldTypes.add(cExpr.getType());
                                break;
                            case "min":
                                Min mExpr = (Min) fExpr;
                                fieldTypes.add(mExpr.getType());
                                break;
                            case "max":
                                Max maxExpr = (Max) fExpr;
                                fieldTypes.add(maxExpr.getType());
                                break;
                                case "first":
                                First first = (First) fExpr;
                                fieldTypes.add(first.getType());
                                break;
                        }
                    }
            );
            if (tiSelectRequest.getGroupBys().size() == 0) {
                isSingledGroup = true;
            }
        }


        public FieldType getFieldType(int index) {
            if(isSingledGroup) {
                if(index == 0) {
                    return StringType.DEF_STRING;
                } else {
                    return this.fieldTypes.get(index-1);
                }
            } else {
                return this.fieldTypes.get(index);
            }
        }

        public List<FieldType> getFieldTypes() {
            List<FieldType> tmp = new ArrayList<>();
            // if singleGroup, then add StringType for decoding SingleGroup
            if(isSingledGroup) {
                tmp.add(StringType.DEF_STRING);
            }
            tmp.addAll(this.fieldTypes);
            return tmp;
        }
    }
}
