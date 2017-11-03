/*
 *
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
 *
 */

package com.pingcap.tikv.types;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.InvalidCodecFormatException;
import com.pingcap.tikv.meta.TiColumnInfo;

public class DurationType extends IntegerType {
  private static final long FACTOR_NANO_SEC_TO_SEC = 1000000;
  static DurationType of(int tp) {
    return new DurationType(tp);
  }

  private DurationType(int tp) {
    super(tp);
  }

  DurationType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  @Override
  public Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag == VARINT_FLAG) {
      long nanoSec = IntegerType.readVarLong(cdi);
      return nanoSec / FACTOR_NANO_SEC_TO_SEC;
    } else if (flag == INT_FLAG) {
      long nanoSec = IntegerType.readLong(cdi);
      return nanoSec / FACTOR_NANO_SEC_TO_SEC;
    } else {
      throw new InvalidCodecFormatException("Invalid Flag type for Time Type: " + flag);
    }
  }

  /**
   * encode a value to cdo per type.
   *
   * @param cdo destination of data.
   * @param encodeType Key or Value.
   * @param value need to be encoded.
   */
  @Override
  public void encodeNotNull(CodecDataOutput cdo, EncodeType encodeType, Object value) {
    long val;
    if (value instanceof Long) {
      val = ((Long) value);
    } else if (value instanceof Integer) {
      val = ((Integer) value);
    } else {
      throw new UnsupportedOperationException("Can not cast Object to Duration");
    }

    IntegerType.writeVarLong(cdo, val);
  }
}
