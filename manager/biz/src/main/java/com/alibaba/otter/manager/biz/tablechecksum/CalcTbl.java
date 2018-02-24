package com.alibaba.otter.manager.biz.tablechecksum;

import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.zip.CRC32;

/**
 * Created by qinlong on 17/7/25.
 */
public class CalcTbl {
    /**
     * 获取表上的主键或唯一键
     *
     * @param jdbcTemplate
     * @param tableName
     * @return
     * @throws Exception
     */
    public static String[] getKey(JdbcTemplate jdbcTemplate, String tableName, String schema) throws Exception {
        String primarySql = "show index from " + schema + "." + tableName + " where Key_name='PRIMARY'";
        String[] indexes = getKeyBysql(jdbcTemplate, primarySql);
        if (indexes.length == 0) {
            String sql = "show index from " + schema + "." + tableName + " where Non_unique=0";
            indexes = getKeyBysql(jdbcTemplate, sql);
        }
        if (indexes.length == 0) {
            throw new Exception("Warning: No PRIMARY or UNIQUE key found in" + tableName);
        }
        return indexes;
    }

    private static String[] getKeyBysql(JdbcTemplate jdbcTemplate, String sql) {
        String[] indexes = (String[]) jdbcTemplate.execute(sql, new PreparedStatementCallback() {
            @Override
            public Object doInPreparedStatement(PreparedStatement preparedStatement) throws SQLException, DataAccessException {
                ResultSet rs = preparedStatement.executeQuery();
                List<String> indexes = new ArrayList<String>();
                while (rs.next()) {
                    indexes.add(rs.getString(5));
                }
                rs.close();
                return indexes.toArray(new String[indexes.size()]);
            }
        });
        return indexes;
    }

    public static String getCols(JdbcTemplate jdbcTemplate, final String tableName, final String schema) {
        String sql = "select GROUP_CONCAT(COLUMN_NAME) from information_schema.COLUMNS where table_name =? and table_schema=?";
        String clos = (String) jdbcTemplate.execute(sql, new PreparedStatementCallback() {
            @Override
            public Object doInPreparedStatement(PreparedStatement preparedStatement) throws SQLException, DataAccessException {
                preparedStatement.setString(1, tableName);
                preparedStatement.setString(2, schema);
                ResultSet rs = preparedStatement.executeQuery();
                String clos = null;
                while (rs.next()) {
                    clos = rs.getString(1);
                }
                rs.close();
                return clos;
            }
        });
        return clos;
    }


    public static ChunkSqlResult makeChunkSql(JdbcTemplate jdbcTemplate, final String tableName, String schema) throws Exception {
        String tableCols = getCols(jdbcTemplate, tableName, schema);
        String[] indexes = getKey(jdbcTemplate, tableName, schema);

        String tUniqKeyCom = String.join(",", indexes);
        String tUniqKeyOrder = String.join(" asc,", indexes) + " asc";

        List<String> tUniqFilterList = new ArrayList<String>();
        int uniqKeySize = indexes.length;

        for (int i = 0; i < uniqKeySize; i++) {
            String tUniqFilterOr = ("(" + indexes[i] + " > ? ");
            for (int m = 0; m < i; m++) {
                tUniqFilterOr += "and " + (indexes[m] + " = ? ");
            }
            tUniqFilterOr += ")";
            tUniqFilterList.add(tUniqFilterOr);
        }

        String tUniqFilter = "";
        if (tUniqFilterList != null) {
            tUniqFilter = String.join(" OR ", tUniqFilterList);
        }

        String chunkSql = "select concat_ws('#'," + tUniqKeyCom + "), CRC32( concat_ws('#', " + tableCols + ") ) from " + schema + "." + tableName +
                " where " + tUniqFilter + " order by " + tUniqKeyOrder + " limit 2000";

        ChunkSqlResult chunkSqlResult = new ChunkSqlResult();
        chunkSqlResult.setIndexSize(indexes.length);
        chunkSqlResult.setChunkSql(chunkSql);
        return chunkSqlResult;
    }

    public static SelectChunkResult selectChunk(JdbcTemplate jdbcTemplate, String sqlChunkraw, final LinkedList<String> startKeys) {
        return (SelectChunkResult) jdbcTemplate.execute(sqlChunkraw, new PreparedStatementCallback() {
            @Override
            public Object doInPreparedStatement(PreparedStatement preparedStatement) throws SQLException, DataAccessException {
                int s = startKeys.size();
                int postion = 1;
                for (int i = 0; i < s; i++) {
                    preparedStatement.setString(postion, startKeys.get(i));
                    postion++;
                    for (int m = 0; m < i; m++) {
                        preparedStatement.setString(postion, startKeys.get(m));
                        postion++;
                    }
                }
                ResultSet rs = preparedStatement.executeQuery();
                LinkedList<String> maxId = new LinkedList<String>();
                String rowsCrc32 = "";
                while (rs.next()) {
                    if (rs.isLast()) {
                        String lastId = rs.getString(1);
                        String[] maxIndexValues = lastId.split("#");
                        for (int k = 0; k < maxIndexValues.length; k++) {
                            maxId.add(maxIndexValues[k]);
                        }
                    }
                    rowsCrc32 += rs.getString(2) + ",";
                }
                rs.close();
                SelectChunkResult selectChunkResult = new SelectChunkResult();
                if (!"".equals(rowsCrc32)) {
                    CRC32 crc32 = new CRC32();
                    crc32.update(rowsCrc32.getBytes());
                    selectChunkResult.setMaxId(maxId);
                    selectChunkResult.setCrc32Value(crc32.getValue());
                } else {
                    selectChunkResult.setMaxId(null);
                    selectChunkResult.setCrc32Value(-1L);
                }
                return selectChunkResult;
            }
        });
    }


    public static Set<String> getChunkRows(JdbcTemplate jdbcTemplate, final String tableName, String schema,
                                           final LinkedList<String> startKeys, final LinkedList<String> endKeys) throws Exception {
        String tableCols = getCols(jdbcTemplate, tableName, schema);
        String[] indexes = getKey(jdbcTemplate, tableName, schema);

        String tUniqKeyCom = String.join(",", indexes);
        String tUniqKeyOrder = String.join(" asc,", indexes) + " asc";


        int uniqKeySize = indexes.length;
        List<String> tUniqFilterListMin = new ArrayList<String>();

        for (int i = 0; i < uniqKeySize; i++) {
            String tUniqFilterOr = ("(" + indexes[i] + " > ? ");
            for (int m = 0; m < i; m++) {
                tUniqFilterOr += "and " + (indexes[m] + " = ? ");
            }
            tUniqFilterOr += ")";
            tUniqFilterListMin.add(tUniqFilterOr);
        }

        String tUniqFilterMin = "";
        if (tUniqFilterListMin != null) {
            tUniqFilterMin = String.join(" OR ", tUniqFilterListMin);
        }

        List<String> tUniqFilterListMax = new ArrayList<String>();
        for (int i = 0; i < uniqKeySize; i++) {
            String tUniqFilterOr = "";
            if (i == uniqKeySize - 1) {
                tUniqFilterOr = ("(" + indexes[i] + " <= ? ");
            } else {
                tUniqFilterOr = ("(" + indexes[i] + " < ? ");
            }
            for (int m = 0; m < i; m++) {
                tUniqFilterOr += "and " + (indexes[m] + " = ? ");
            }
            tUniqFilterOr += ")";
            tUniqFilterListMax.add(tUniqFilterOr);
        }

        String tUniqFilterMax = "";
        if (tUniqFilterListMax != null) {
            tUniqFilterMax = String.join(" OR ", tUniqFilterListMax);
        }

        String tUniqFilter = String.format("(%s) AND (%s)", tUniqFilterMin, tUniqFilterMax);

        String sqlPlainRows = "select concat_ws('#'," + tUniqKeyCom + "), CRC32( concat_ws('#', " + tableCols + ") ) from " + schema + "." + tableName +
                " where " + tUniqFilter + " order by " + tUniqKeyOrder;

        return jdbcTemplate.execute(sqlPlainRows, new PreparedStatementCallback<Set<String>>() {
            @Override
            public Set<String> doInPreparedStatement(PreparedStatement preparedStatement) throws SQLException, DataAccessException {
                int s = startKeys.size();
                int postion = 1;
                for (int i = 0; i < s; i++) {
                    preparedStatement.setString(postion, startKeys.get(i));
                    postion++;
                    for (int m = 0; m < i; m++) {
                        preparedStatement.setString(postion, startKeys.get(m));
                        postion++;
                    }
                }

                s = endKeys.size();
                for (int i = 0; i < s; i++) {
                    preparedStatement.setString(postion, endKeys.get(i));
                    postion++;
                    for (int m = 0; m < i; m++) {
                        preparedStatement.setString(postion, endKeys.get(m));
                        postion++;
                    }
                }
                ResultSet rs = preparedStatement.executeQuery();
                Set<String> result = new HashSet<String>();
                while (rs.next()) {
                    result.add(rs.getString(1) + ":" + rs.getString(2));
                }
                rs.close();

                return result;
            }
        });
    }

    public static void fixTargetDbTable(final JdbcTemplate sourceJdbcTemplate, final JdbcTemplate targetJdbcTemplate,
                                        final String tableName, final String schema, final FixSqlKey fixSqlKey, Table table) throws Exception {
        String[] indexes = getKey(sourceJdbcTemplate, tableName, schema);
        final Column[] tableColumns = table.getColumns();

        String keyStr = String.join("=? and ", indexes) + " = ? ";
        if (fixSqlKey.getDeleteKeys().size() > 0) {
            for (final String key : fixSqlKey.getDeleteKeys()) {
                targetJdbcTemplate.update("DELETE FROM " + schema + "." + tableName + " where " + keyStr, new PreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement preparedStatement) throws SQLException {
                        String[] k = key.split("#");
                        for (int i = 1; i <= k.length; i++) {
                            preparedStatement.setString(i, k[i - 1]);
                        }
                    }
                });
            }
        }
        if (fixSqlKey.getInsertOrUpdateKeys().size() > 0) {
            final String selectRaw = "SELECT * FROM " + schema + "." + tableName + " where " + keyStr;

            List<String> places = new ArrayList<String>();
            for (int i = 1; i <= tableColumns.length; i++) {
                places.add("?");
            }
            String insertSqlRaw = "REPLACE into " + schema + "." + tableName + " values (" + String.join(",", places) + ")";

            final List<String> keyList = new ArrayList<String>(fixSqlKey.getInsertOrUpdateKeys());

            BatchPreparedStatementSetter setter = new BatchPreparedStatementSetter() {
                @Override
                public void setValues(final PreparedStatement updatePreparedStatement, final int i) throws SQLException {
                    sourceJdbcTemplate.execute(selectRaw, new PreparedStatementCallback() {
                        @Override
                        public Object doInPreparedStatement(PreparedStatement preparedStatement) throws SQLException, DataAccessException {
                            String[] k = keyList.get(i).split("#");
                            for (int s = 1; s <= k.length; s++) {
                                preparedStatement.setString(s, k[s - 1]);
                            }
                            final ResultSet rs = preparedStatement.executeQuery();
                            while (rs.next()) {
                                for (int m = 1; m <= tableColumns.length; m++) {
                                    StatementCreatorUtils.setParameterValue(updatePreparedStatement, m,
                                            tableColumns[m - 1].getTypeCode(), tableColumns[m - 1].getType(), rs.getObject(m));
                                }
                            }
                            return null;
                        }
                    });
                }

                @Override
                public int getBatchSize() {
                    return fixSqlKey.getInsertOrUpdateKeys().size();
                }
            };
            targetJdbcTemplate.batchUpdate(insertSqlRaw, setter);
            System.out.print("成功批量 REPLACE into.........");


            /*for (final String key : fixSqlKey.getInsertOrUpdateKeys()) {
                sourceJdbcTemplate.execute("SELECT * FROM " + schema + "." + tableName + " where " + keyStr, new PreparedStatementCallback() {
                    @Override
                    public Object doInPreparedStatement(PreparedStatement preparedStatement) throws SQLException, DataAccessException {
                        String[] k = key.split("-");
                        for (int i = 1; i <= k.length; i++) {
                            preparedStatement.setString(i, k[i - 1]);
                        }

                        List<String> places = new ArrayList<String>();
                        for (int i = 1; i <= tableColumns.length; i++) {
                            places.add("?");
                        }

                        String insertSqlRaw = "REPLACE into " + schema + "." + tableName + " values (" + String.join(",", places) + ")";
                        final ResultSet rs = preparedStatement.executeQuery();

                        while (rs.next()) {
                            targetJdbcTemplate.update(insertSqlRaw, new PreparedStatementSetter() {
                                @Override
                                public void setValues(PreparedStatement preparedStatement) throws SQLException {
                                    for (int i = 1; i <= tableColumns.length; i++) {
                                        StatementCreatorUtils.setParameterValue(preparedStatement, i,
                                                tableColumns[i - 1].getTypeCode(), tableColumns[i - 1].getType(), rs.getObject(i));
                                    }
                                }
                            });
                            System.out.print("成功 REPLACE into.........");
                        }
                        return null;
                    }
                });
            }*/
        }

    }
}
