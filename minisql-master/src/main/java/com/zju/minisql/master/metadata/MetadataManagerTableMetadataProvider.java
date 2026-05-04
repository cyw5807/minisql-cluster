package com.zju.minisql.master.metadata;

import com.zju.minisql.common.meta.TableMeta;
import com.zju.minisql.master.meta.MetadataManager;

/**
 * 基于组长现有 MetadataManager 的适配器。
 */
public class MetadataManagerTableMetadataProvider implements TableMetadataProvider {

    private final MetadataManager metadataManager;

    public MetadataManagerTableMetadataProvider(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    @Override
    public TableMeta getTable(String tableName) throws Exception {
        return metadataManager.getTable(tableName);
    }
}
