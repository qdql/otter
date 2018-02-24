package com.alibaba.otter.manager.biz.service;

import com.alibaba.otter.manager.biz.BaseOtterTest;
import com.alibaba.otter.manager.biz.config.channel.ChannelService;
import com.alibaba.otter.manager.biz.config.datamediapair.DataMediaPairService;
import com.alibaba.otter.manager.biz.config.record.LogRecordService;
import com.alibaba.otter.manager.biz.tablechecksum.CheckSumService;
import com.alibaba.otter.shared.arbitrate.impl.config.ArbitrateConfigUtils;
import com.alibaba.otter.shared.common.model.config.channel.Channel;
import com.alibaba.otter.shared.common.model.config.data.ColumnGroup;
import com.alibaba.otter.shared.common.model.config.data.ColumnPair;
import com.alibaba.otter.shared.common.model.config.data.DataMediaPair;
import com.alibaba.otter.shared.common.model.config.node.Node;
import org.jtester.annotations.SpringBeanByName;
import org.springframework.util.CollectionUtils;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Created by qinlong on 17/7/21.
 */
public class DataMediaPairInfoTest extends BaseOtterTest {
    @SpringBeanByName
    private ChannelService channelService;

    @SpringBeanByName
    private DataMediaPairService dataMediaPairService;

    @SpringBeanByName
    private LogRecordService logRecordService;

    @SpringBeanByName
    private CheckSumService checkSumService;

    @Test
    public void testDataMediaPair() {
        DataMediaPair dataMediaPair = dataMediaPairService.findById(37L);
        Channel channel = channelService.findByPipelineId(dataMediaPair.getPipelineId());

        List<ColumnPair> columnPairs = dataMediaPair.getColumnPairs();
        List<ColumnGroup> columnGroups = dataMediaPair.getColumnGroups();
        // 暂时策略，只拿出list的第一个Group
        ColumnGroup columnGroup = new ColumnGroup();
        if (!CollectionUtils.isEmpty(columnGroups)) {
            columnGroup = columnGroups.get(0);
        }
    }

    @Test
    public void testNodeInfo() {
        Node node = ArbitrateConfigUtils.findNode(1L);
        Channel channel = ArbitrateConfigUtils.getChannelByChannelId(2L);
        List<Channel> channels = channelService.listAll();
        System.out.println(node);
    }




    @Test
    public void testJdbcTemplate() {
        checkSumService.doCheckSum();
    }
}
