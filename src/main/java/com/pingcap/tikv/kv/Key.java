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

package com.pingcap.tikv.kv;

import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

// Key represents high-level Key type.
public class Key {
    public Key() {
       this.data = new ArrayList<>();
    }

    public Key(ByteString bs) {
        this.data = new ArrayList<>(Byte.parseByte(bs.toString()));
    }

    public Key(int size) {
        this.data = new ArrayList<>(size);
    }

   private List<Byte> data;

    public Key(Key another) {
       this.data = another.data;
    }

   // Next returns the next key in byte-order.
   public Key next() {
       this.data.add((byte)0);
       this.data.remove(0);
      return this;
   }

   // PrefixNext returns the next prefix key.
   //
   // Assume there are keys like:
   //
   //   rowkey1
   //   rowkey1_column1
   //   rowkey1_column2
   //   rowKey2
   //
   // If we seek 'rowkey1' Next, we will get 'rowkey1_colum1'.
   // If we seek 'rowkey1' PrefixNext, we will get 'rowkey2'.
   public Key prefixNext() {
       Key another = new Key(this);
       int i = this.size();
       for(; i >= 0; i--) {
           this.set(i, (byte) (this.get(i)+1));
           if (this.get(i) != (byte) 0) {
               break;
           }
       }
       // if i i -1, it means is not prefix found.
       if (i == -1) {
           another.data.add((byte)0);
           return another;
       }
      return this;
   }

   public boolean compare(Key another) {
       return this.data.toString().contentEquals(another.data.toString());
   }

   public int size() {
       return this.data.size();
   }

   public byte get(int i) {
       return this.data.get(i);
   }

   public void put(byte... bs) {
       for (byte b: bs) {
          this.data.add(b);
       }
   }

   /*
    * put add bytes into data according to the size. If size is larger, then whole bs will be written.
    * Otherwise, only bs[0:size] will be written into data.
    * @param bs source of data
    * @param size is how much element will be written in.
    */
   public void put(byte[] bs, int size) {
       int len = size > bs.length? size: bs.length;
       for (int i = 0; i < len; i++) {
          this.data.add(bs[i]);
       }
   }

   public void set(int i, byte b) {
        this.data.set(i, b);
   }

   public boolean hasPrefix(Key prefix) {
       for (int i = 0; i < prefix.size(); i++) {
           if (this.get(i) != prefix.get(i)) {
               return false;
           }
       }
       return true;
   }

   public byte[] toByteArray() {
       return Bytes.toArray(this.data);
   }

    public ByteString toByteString() {
        return ByteString.copyFrom(toByteArray());
    }

    public class KeyRange {
     private Key startKey;
     private Key endKey;

        public KeyRange(Key startKey, Key endKey) {
            this.startKey = startKey;
            this.endKey = endKey;
        }

        public boolean isPoint() {
         return startKey.prefixNext().compare(endKey);
     }
  }
}
