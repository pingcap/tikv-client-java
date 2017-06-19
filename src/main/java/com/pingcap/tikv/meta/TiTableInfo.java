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


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.pingcap.tidb.tipb.TableInfo;

import java.util.List;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TiTableInfo {
    private final long                id;
    private final String              name;
    private final String              charset;
    private final String              collate;
    private final List<TiColumnInfo>  columns;
    private final List<TiIndexInfo>   indices;
    private final boolean             pkIsHandle;
    private final String              comment;
    private final long                autoIncId;
    private final long                maxColumnId;
    private final long                maxIndexId;
    private final long                oldSchemaId;

    @JsonCreator
    public TiTableInfo(@JsonProperty("id")long                          id,
                       @JsonProperty("name")CIStr                       name,
                       @JsonProperty("charset")String                   charset,
                       @JsonProperty("collate")String                   collate,
                       @JsonProperty("pk_is_handle")boolean             pkIsHandle,
                       @JsonProperty("cols")List<TiColumnInfo>          columns,
                       @JsonProperty("index_info")List<TiIndexInfo>     indices,
                       @JsonProperty("comment")String                   comment,
                       @JsonProperty("auto_inc_id")long                 autoIncId,
                       @JsonProperty("max_col_id")long                  maxColumnId,
                       @JsonProperty("max_idx_id")long                  maxIndexId,
                       @JsonProperty("old_schema_id")long               oldSchemaId) {
        this.id = id;
        this.name = name.getL();
        this.charset = charset;
        this.collate = collate;
        this.columns = columns;
        this.pkIsHandle = pkIsHandle;
        this.indices = indices;
        this.comment = comment;
        this.autoIncId = autoIncId;
        this.maxColumnId = maxColumnId;
        this.maxIndexId = maxIndexId;
        this.oldSchemaId = oldSchemaId;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getCharset() {
        return charset;
    }

    public String getCollate() {
        return collate;
    }

    public List<TiColumnInfo> getColumns() {
        return columns;
    }

    public boolean isPkHandle() {
        return pkIsHandle;
    }

    public List<TiIndexInfo> getIndices() {
        return indices;
    }

    public String getComment() {
        return comment;
    }

    public long getAutoIncId() {
        return autoIncId;
    }

    public long getMaxColumnId() {
        return maxColumnId;
    }

    public long getMaxIndexId() {
        return maxIndexId;
    }

    public long getOldSchemaId() {
        return oldSchemaId;
    }

    public TableInfo toProto() {
        return TableInfo.newBuilder()
                .setTableId(getId())
                .addAllColumns(getColumns().stream().map(TiColumnInfo::toProto).collect(Collectors.toList()))
                .build();
    }
}
