package com.alibaba.otter.manager.biz.tablechecksum;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * Created by qinlong on 17/7/29.
 */
@Component
@EnableScheduling
public class CheckSumTask {
    private static final Logger logger               = LoggerFactory.getLogger("monitorTrigger");

    @Resource(name = "checkSumService")
    private CheckSumService checkSumService;

    @Scheduled(cron = "0 0/30 * * * ?")
    public void doCheckSumTask() {
        logger.info("#执行check sum");
        checkSumService.doCheckSum();
    }
}
