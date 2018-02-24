package com.alibaba.otter.manager.biz.tablechecksum;

import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.model.Table;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * Created by qinlong on 17/7/26.
 */
public class CheckSum {
    public static void doCheckSum(JdbcTemplate sourceJdbcTemplate, JdbcTemplate targetJdbcTemplate,
                                  String tableName, String schema, Table table) throws Exception {
        ChunkSqlResult chunkSqlResult = CalcTbl.makeChunkSql(sourceJdbcTemplate, tableName, schema);
        String sqlChunkraw = chunkSqlResult.getChunkSql();
        LinkedList<String> startKeyList = new LinkedList<String>();
        for (int i = 0; i < chunkSqlResult.getIndexSize(); i++) {
            startKeyList.add("0");
        }
        while (true) {
            SelectChunkResult selectChunkResult = CalcTbl.selectChunk(sourceJdbcTemplate, sqlChunkraw, startKeyList);
            if (selectChunkResult.getCrc32Value().longValue() != -1L) {
                //目标库计算
                SelectChunkResult targetSelectChunkResult = CalcTbl.selectChunk(targetJdbcTemplate, sqlChunkraw, startKeyList);
                if (targetSelectChunkResult.getCrc32Value().longValue() == selectChunkResult.getCrc32Value().longValue()) {
                    System.out.println("对比一致");
                } else {
                    System.out.println("对比不一致");
                    LinkedList<String> endKeys;
                    if (targetSelectChunkResult.getMaxId() == null) {
                        endKeys = selectChunkResult.getMaxId();
                    } else {
                        endKeys = getRealMaxId(selectChunkResult.getMaxId(), targetSelectChunkResult.getMaxId());
                    }

                    Set<String> sourceDbRows = CalcTbl.getChunkRows(sourceJdbcTemplate, tableName, schema, startKeyList, endKeys);
                    Set<String> targetDbRows = CalcTbl.getChunkRows(targetJdbcTemplate, tableName, schema, startKeyList, endKeys);
                    System.out.println(sourceDbRows);
                    System.out.println(targetDbRows);

                    // inserted or updated
                    Set<String> insertOrUpdated = new HashSet<String>();
                    insertOrUpdated.addAll(sourceDbRows);
                    insertOrUpdated.removeAll(targetDbRows);
                    Set<String> insertOrUpdatedKeys = new HashSet<String>();
                    if (insertOrUpdated.size() > 0) {
                        for (String item : insertOrUpdated) {
                            insertOrUpdatedKeys.add(item.split(":")[0]);
                        }
                    }

                    // deleted or updated
                    Set<String> deletedOrUpdated = new HashSet<String>();
                    deletedOrUpdated.addAll(targetDbRows);
                    deletedOrUpdated.removeAll(sourceDbRows);
                    Set<String> deletedOrUpdatedKeys = new HashSet<String>();
                    if (deletedOrUpdated.size() > 0) {
                        for (String item : deletedOrUpdated) {
                            deletedOrUpdatedKeys.add(item.split(":")[0]);
                        }
                    }

                    // updated
                    Set<String> updated = new HashSet<String>();
                    updated.addAll(insertOrUpdatedKeys);
                    updated.retainAll(deletedOrUpdatedKeys);

                    // deleted
                    Set<String> deleted = new HashSet<String>();
                    deleted.addAll(deletedOrUpdatedKeys);
                    deleted.removeAll(updated);

                    // insert
                    Set<String> insert = new HashSet<String>();
                    insert.addAll(insertOrUpdatedKeys);
                    insert.removeAll(updated);

                    System.out.println("updated:");
                    System.out.println(updated);

                    System.out.println("deleted:");
                    System.out.println(deleted);

                    System.out.println("insert:");
                    System.out.println(insert);

                    FixSqlKey fixSqlKey = new FixSqlKey();
                    fixSqlKey.setDeleteKeys(deleted);
                    updated.addAll(insert);
                    fixSqlKey.setInsertOrUpdateKeys(updated);

                    try {
                        CalcTbl.fixTargetDbTable(sourceJdbcTemplate, targetJdbcTemplate, tableName, schema, fixSqlKey, table);
                        System.out.println("目标库执行sql成功......");
                    } catch (Exception e) {
                        StringBuilder builder = new StringBuilder();
                        builder.append("目标库");
                        builder.append(schema).append(".").append(table);
                        builder.append("执行sql失败");
                        builder.append("\n");
                        if (fixSqlKey.getInsertOrUpdateKeys().size() > 0) {
                            builder.append("insert or update keys:");
                            builder.append(StringUtils.join(fixSqlKey.getInsertOrUpdateKeys(), ","));
                        }
                        if (fixSqlKey.getDeleteKeys().size() > 0) {
                            builder.append("\n");
                            builder.append("delete keys:");
                            builder.append(StringUtils.join(fixSqlKey.getDeleteKeys(), ","));
                        }
                        throw new Exception(builder.toString());
                    }

                    //break;
                }
                startKeyList = selectChunkResult.getMaxId();
            } else {
                System.out.println("源实例" + tableName + " 计算checksum结束！");
                break;
            }
        }

    }

    private static LinkedList<String> getRealMaxId(LinkedList<String> maxId1, LinkedList<String> maxId2) {
        int size1 = maxId1.size();
        int size2 = maxId2.size();
        if (size1 > size2) {
            return maxId1;
        }
        if (size2 > size1) {
            return maxId2;
        }
        LinkedList<String> result = null;
        for (int i = 0; i < size1; i++) {
            if (maxId1.get(i).compareTo(maxId2.get(i)) > 0) {
                result = maxId1;
            }
            if (maxId1.get(i).compareTo(maxId2.get(i)) < 0) {
                result = maxId2;
            }
            if (result != null) {
                break;
            }
        }
        if (result == null) {
            result = maxId1;
        }
        return result;
    }
}
