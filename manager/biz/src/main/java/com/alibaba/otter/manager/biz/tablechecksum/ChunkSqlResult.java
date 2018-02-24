package com.alibaba.otter.manager.biz.tablechecksum;

/**
 * Created by qinlong on 17/7/26.
 */
public class ChunkSqlResult {
    private int indexSize;
    private String chunkSql;

    public int getIndexSize() {
        return indexSize;
    }

    public void setIndexSize(int indexSize) {
        this.indexSize = indexSize;
    }

    public String getChunkSql() {
        return chunkSql;
    }

    public void setChunkSql(String chunkSql) {
        this.chunkSql = chunkSql;
    }
}
