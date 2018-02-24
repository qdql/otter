package com.alibaba.otter.manager.biz.tablechecksum;

import com.alibaba.otter.manager.biz.config.channel.ChannelService;
import com.alibaba.otter.manager.biz.config.record.LogRecordService;
import com.alibaba.otter.manager.biz.utils.DataSourceChecker;
import com.alibaba.otter.shared.common.model.config.channel.Channel;
import com.alibaba.otter.shared.common.model.config.channel.ChannelStatus;
import com.alibaba.otter.shared.common.model.config.data.DataMediaPair;
import com.alibaba.otter.shared.common.model.config.data.db.DbDataMedia;
import com.alibaba.otter.shared.common.model.config.pipeline.Pipeline;
import com.alibaba.otter.shared.common.model.config.record.LogRecord;
import com.alibaba.otter.shared.common.utils.meta.DdlUtils;
import org.apache.ddlutils.model.Table;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by qinlong on 17/7/29.
 */
public class CheckSumService {
    private ChannelService channelService;

    private LogRecordService logRecordService;

    private JdbcTemplate jdbcTemplate;

    private JdbcTemplate targetJdbcTemplate;

    public void doCheckSum() {
        List<Channel> channels = channelService.listAll();

        LinkedList<DbDataMedia> sourceDbDataMedias = new LinkedList<DbDataMedia>();
        final LinkedList<DbDataMedia> targetDbDataMedias = new LinkedList<DbDataMedia>();

        if (channels.size() > 0) {
            for (Channel channel : channels) {
                if (channel.getStatus() == ChannelStatus.START) {
                    if (channel.getPipelines().size() > 0) {
                        for (Pipeline pipeline : channel.getPipelines()) {
                            if (pipeline.getPairs().size() > 0) {
                                for (DataMediaPair dataMediaPair : pipeline.getPairs()) {
                                    if (dataMediaPair.getSource() != null) {
                                        DbDataMedia sourceDbDataMedia = (DbDataMedia) dataMediaPair.getSource();
                                        sourceDbDataMedias.add(sourceDbDataMedia);
                                    }
                                    if (dataMediaPair.getTarget() != null) {
                                        DbDataMedia targetDbDataMedia = (DbDataMedia) dataMediaPair.getTarget();
                                        targetDbDataMedias.add(targetDbDataMedia);
                                    }

                                }
                            }
                        }
                    }
                }
            }
        }
        //测试直选一个
        //List<DbDataMedia> testSourceDbDataMedias = sourceDbDataMedias.subList(0, 10);

        int num = sourceDbDataMedias.size();
        ExecutorService executor = Executors.newCachedThreadPool();
        final CountDownLatch count = new CountDownLatch(num);
        int i = 0;
        if (num > 0) {
            if (jdbcTemplate == null) {
                DbDataMedia dbDataMedia = sourceDbDataMedias.get(0);
                DataSource dataSource = DataSourceChecker.createDataSource(dbDataMedia.getSource().getUrl(),
                        dbDataMedia.getSource().getUsername(), dbDataMedia.getSource().getPassword(),
                        dbDataMedia.getSource().getDriver(), dbDataMedia.getSource().getType(),
                        dbDataMedia.getSource().getEncode());
                jdbcTemplate = new JdbcTemplate(dataSource);
            }

            if (targetJdbcTemplate == null) {
                DbDataMedia targetDbDataMedia = targetDbDataMedias.get(0);
                DataSource targetDataSource = DataSourceChecker.createDataSource(targetDbDataMedia.getSource().getUrl(),
                        targetDbDataMedia.getSource().getUsername(), targetDbDataMedia.getSource().getPassword(),
                        targetDbDataMedia.getSource().getDriver(), targetDbDataMedia.getSource().getType(),
                        targetDbDataMedia.getSource().getEncode());
                targetJdbcTemplate = new JdbcTemplate(targetDataSource);
            }
        }
        for (final DbDataMedia dbDataMedia : sourceDbDataMedias) {
            final int finalI = i;
            final JdbcTemplate finalJdbcTemplate = jdbcTemplate;
            final JdbcTemplate finalTargetJdbcTemplate = targetJdbcTemplate;
            executor.submit(new Callable() {
                public Object call() throws Exception {
                    Thread.currentThread().setName(dbDataMedia.getName() + " check sum thread");
                    try {
//                        if ("partner_station".equals(dbDataMedia.getName())) {
                        if (true) {
//                            DataSource dataSource = DataSourceChecker.createDataSource(dbDataMedia.getSource().getUrl(),
//                                    dbDataMedia.getSource().getUsername(), dbDataMedia.getSource().getPassword(),
//                                    dbDataMedia.getSource().getDriver(), dbDataMedia.getSource().getType(),
//                                    dbDataMedia.getSource().getEncode());
//                            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
//
//
//                            DbDataMedia targetDbDataMedia = targetDbDataMedias.get(finalI);
//                            DataSource targetDataSource = DataSourceChecker.createDataSource(targetDbDataMedia.getSource().getUrl(),
//                                    targetDbDataMedia.getSource().getUsername(), targetDbDataMedia.getSource().getPassword(),
//                                    targetDbDataMedia.getSource().getDriver(), targetDbDataMedia.getSource().getType(),
//                                    targetDbDataMedia.getSource().getEncode());
//                            JdbcTemplate targetJdbcTemplate = new JdbcTemplate(targetDataSource);

                            DbDataMedia targetDbDataMedia = targetDbDataMedias.get(finalI);
                            try {
                                if (finalJdbcTemplate != null) {
                                    finalJdbcTemplate.execute("use " + dbDataMedia.getNamespace());
                                }
                                if (finalTargetJdbcTemplate != null) {
                                    finalTargetJdbcTemplate.execute("use " + targetDbDataMedia.getNamespace());
                                }
                            } catch (DataAccessException e) {
                                e.printStackTrace();
                            }
                            try {
                                Table table = DdlUtils.findTable(finalJdbcTemplate, dbDataMedia.getNamespace(),
                                        dbDataMedia.getNamespace(), dbDataMedia.getName());
                                CheckSum.doCheckSum(finalJdbcTemplate, finalTargetJdbcTemplate, dbDataMedia.getName(),
                                        dbDataMedia.getNamespace(), table);
                            } catch (Exception e) {
                                LogRecord logRecord = new LogRecord();
                                logRecord.setTitle("CHECKSUM");
                                logRecord.setNid(-1L);
                                logRecord.setPipeline(null);
                                logRecord.setMessage(e.getMessage());
                                logRecordService.create(logRecord);
                                e.printStackTrace();
                            }

                        }
                        count.countDown();
                        return null;
                    } finally {
                    }
                }
            });
            i++;
        }
        try {
            count.await();
            executor.shutdown();
        } catch (InterruptedException e) {
        }
    }

    public LogRecordService getLogRecordService() {
        return logRecordService;
    }

    public void setLogRecordService(LogRecordService logRecordService) {
        this.logRecordService = logRecordService;
    }

    public ChannelService getChannelService() {
        return channelService;
    }

    public void setChannelService(ChannelService channelService) {
        this.channelService = channelService;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public JdbcTemplate getTargetJdbcTemplate() {
        return targetJdbcTemplate;
    }

    public void setTargetJdbcTemplate(JdbcTemplate targetJdbcTemplate) {
        this.targetJdbcTemplate = targetJdbcTemplate;
    }
}
