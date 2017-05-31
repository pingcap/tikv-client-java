package com.pingcap.tikv.operation;


import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.types.FieldType;
import com.pingcap.tikv.types.VarCharType;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

public class SchemaInfer {

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
                        fieldTypes.add(expr.getType());
                    }
            );
            if (tiSelectRequest.getGroupBys().size() == 0) {
                isSingledGroup = true;
            }
        }

        public FieldType getFieldType(int index) {
            if(isSingledGroup) {
                if(index == 0) {
                    return VarCharType.DEF_TYPE;
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
                tmp.add(VarCharType.DEF_TYPE);
            }
            tmp.addAll(this.fieldTypes);
            return tmp;
        }
    }
}
