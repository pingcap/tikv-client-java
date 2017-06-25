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

package com.pingcap.tikv.operation;

import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

import static com.pingcap.tikv.types.Types.TYPE_VARCHAR;

/**
 * SchemaInfer extract row's type after query is executed.
 * It is pretty rough version for. Optimization is on the way.
 * The problem we have right now is that TiDB promote Sum to Decimal which is
 * not compatible with column's type.
 * The solution we come up with right now is use record column's type ad finalFieldType
 * and build another list recording TiExpr's type as fieldType for row reading.
 * Once we finish row reading, we first check each element in fieldType and finalFieldType share
 * the same type or not. If yes, no need for casting. If no, casting is needed here.
 */
public class SchemaInfer {
    @Getter
    private List<DataType> types;

    public static SchemaInfer create(TiSelectRequest tiSelectRequest) {
        return new SchemaInfer(tiSelectRequest);
    }

    public SchemaInfer(TiSelectRequest tiSelectRequest) {
        types = new ArrayList<>();
        extractFieldTypes(tiSelectRequest);
    }

    /**
     * TODO: order by
     * extract field types from tiSelectRequest for reading data to row.
     *
     * @param tiSelectRequest is SelectRequest
     */
    private void extractFieldTypes(TiSelectRequest tiSelectRequest) {
        tiSelectRequest.getGroupBys().forEach(
                groupBy -> {
                    types.add(DataTypeFactory.of(TYPE_VARCHAR));
                }
        );

        if (tiSelectRequest.getAggregates().size() > 0) {
            if (tiSelectRequest.getGroupBys().size() == 0) {
                types.add(DataTypeFactory.of(TYPE_VARCHAR));
            }
        }

        // Extract all column type information from TiExpr
        // Since fields are simple expression, intermediateFieldTypes shares the same
        // FieldType information.
        tiSelectRequest.getFields().forEach(
                expr -> {
                    types.add(expr.getType());
                }
        );

        tiSelectRequest.getAggregates().forEach(
                expr -> {
                    types.add(expr.getType());
                }
        );
    }

    public DataType getType(int index) {
        return types.get(index);
    }
}
