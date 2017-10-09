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

package com.pingcap.tikv.meta;

import com.pingcap.tikv.exception.TiClientInternalException;

public enum IndexType {
  IndexTypeInvalid(0),
  IndexTypeBtree(1),
  IndexTypeHash(2);

  private final int type;

  IndexType(int type) {
    this.type = type;
  }

  public static IndexType fromValue(int type) {
    for (IndexType e : IndexType.values()) {
      if (e.type == type) {
        return e;
      }
    }
    throw new TiClientInternalException("Invalid index type code: " + type);
  }

  public int getTypeCode() {
    return type;
  }

  public String toString() {
    switch (this.type){
      case 1:
        return "BTREE";
      case 2:
        return "HASH";
    }
    return "Invalid";
  }
}