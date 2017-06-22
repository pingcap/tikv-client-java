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

package com.pingcap.tikv.predicates;


import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.And;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PredicateUtils {
    public static TiExpr mergeCNFExpressions(List<TiExpr> exprs) {
        requireNonNull(exprs);
        if (exprs.size() == 0) return null;
        if (exprs.size() == 1) return exprs.get(0);

        return new And(exprs.get(0), mergeCNFExpressions(exprs.subList(1, exprs.size())));
    }
}
