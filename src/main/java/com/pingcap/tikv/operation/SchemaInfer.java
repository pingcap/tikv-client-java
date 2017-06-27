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

import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.meta.TiSelectRequest;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataTypeFactory;

import java.util.ArrayList;
import java.util.List;

import static com.pingcap.tikv.types.Types.TYPE_VARCHAR;

/**
 * SchemaInfer extract row's type after query is executed.
 * It is pretty rough version. Optimization is on the way.
 * The problem we have right now is that TiDB promote Sum to Decimal which is
 * not compatible with column's type.
 * The solution we come up with right now is use record column's type ad finalFieldType
 * and build another list recording TiExpr's type as fieldType for row reading.
 * Once we finish row reading, we first check each element in fieldType and finalFieldType share
 * the same type or not. If yes, no need for casting. If no, casting is needed here.
 */
public class SchemaInfer {
    private List<DataType> types;
    public static SchemaInfer create(TiSelectRequest tiSelectRequest) {
        return new SchemaInfer(tiSelectRequest);
    }

    private SchemaInfer(TiSelectRequest tiSelectRequest) {
        types = new ArrayList<>();
        extractFieldTypes(tiSelectRequest);
    }

    /**
     * TODO: order by
     * extract field types from tiSelectRequest for reading data to row.
     * @param tiSelectRequest is SelectRequest
     */
    private void extractFieldTypes(TiSelectRequest tiSelectRequest) {
        List<TiExpr> groupByExprs = new ArrayList<>();
        tiSelectRequest.getGroupByItems().forEach(
               groupBy -> {
                   groupByExprs.add(groupBy.getExpr());
                   types.add(groupBy.getExpr().getType());
               }
        );

        if (tiSelectRequest.getAggregates().size() > 0) {
            // In some cases, aggregates come without group by clause, we need add a dummy
            // single group for it.
            if(tiSelectRequest.getGroupByItems().size() == 0) {
                types.add(DataTypeFactory.of(TYPE_VARCHAR));
            }
        }

        if (tiSelectRequest.getAggregates().size() == 0) {
            // Extract all column type information from TiExpr
            tiSelectRequest.getFields().forEach(
                    expr -> types.add(expr.getType())
            );
        }

        tiSelectRequest.getAggregates().forEach(
                expr -> types.add(expr.getType())
        );
    }

    public DataType getType(int index) {
        return types.get(index);
    }

    public List<DataType> getTypes() {
        return types;
    }

}
