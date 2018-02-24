package com.alibaba.otter.manager.biz.tablechecksum;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by qinlong on 17/7/27.
 */
public class FixSqlKey {
    private Set<String> insertOrUpdateKeys = new HashSet<String>();
    private Set<String> deleteKeys = new HashSet<String>();

    public Set<String> getInsertOrUpdateKeys() {
        return insertOrUpdateKeys;
    }

    public void setInsertOrUpdateKeys(Set<String> insertOrUpdateKeys) {
        this.insertOrUpdateKeys = insertOrUpdateKeys;
    }

    public Set<String> getDeleteKeys() {
        return deleteKeys;
    }

    public void setDeleteKeys(Set<String> deleteKeys) {
        this.deleteKeys = deleteKeys;
    }
}
