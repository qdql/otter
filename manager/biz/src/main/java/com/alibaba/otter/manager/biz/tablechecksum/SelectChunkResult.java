package com.alibaba.otter.manager.biz.tablechecksum;

import java.util.LinkedList;

/**
 * Created by qinlong on 17/7/26.
 */
public class SelectChunkResult {
    LinkedList<String> maxId;
    Long crc32Value;

    public LinkedList<String> getMaxId() {
        return maxId;
    }

    public void setMaxId(LinkedList<String> maxId) {
        this.maxId = maxId;
    }

    public Long getCrc32Value() {
        return crc32Value;
    }

    public void setCrc32Value(Long crc32Value) {
        this.crc32Value = crc32Value;
    }
}
