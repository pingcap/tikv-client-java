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

import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.CastingException;

import java.nio.ByteBuffer;

/**
 * Wrap objects into comparable types
 */
public class Comparables {
    public static Comparable<?> wrap(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof Comparable<?>) {
            return (Comparable<?>)o;
        }
        if (o instanceof byte[]) {
            return ByteBuffer.wrap((byte[])o);
        }
        if (o instanceof ByteString) {
            return ((ByteString)o).asReadOnlyByteBuffer();
        }
        throw new CastingException("Cannot cast to Comparable for type: " + o.getClass().getSimpleName());
    }
}
