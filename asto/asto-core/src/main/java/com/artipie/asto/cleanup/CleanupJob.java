/*
 * The MIT License (MIT) Copyright (c) 2020-2023 artipie.com
 * https://github.com/artipie/artipie/blob/master/LICENSE.txt
 */
package com.artipie.asto.cleanup;

import com.artipie.asto.Storage;
import com.artipie.asto.factory.Config;
import com.jcabi.log.Logger;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.time.Duration;

@DisallowConcurrentExecution
public class CleanupJob implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        final Storage storage = (Storage) context.get("storage");
        final CleanupPolicy policy = (CleanupPolicy) context.get("policy");
        Cleaner.cleanup(storage, policy);
    }

    public static void schedule(Storage storage, Config cfg) {
        if(cfg.isEmpty()) {
            return;
        }
        var cron = cfg.config("on").string("cron");
        var policy = new CleanupPolicy();
        policy.setMaxAge(Duration.parse(cfg.string("maxAge")));
        policy.setMaxUnused(Duration.parse(cfg.string("maxUnused")));
        if(!cron.isEmpty()) {
            var data = new JobDataMap();
            data.put("storage", storage);
            data.put("policy", policy);
            final JobDetail job = JobBuilder
                    .newJob()
                    .ofType(CleanupJob.class)
                    .withIdentity(String.format("%s-%s-%s", storage.identifier(), cron, CleanupJob.class.getCanonicalName()))
                    .setJobData(data)
                    .build();
            final Trigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity(
                            String.format("trigger-%s", job.getKey()),
                            "cleanup-group"
                    )
                    .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                    .forJob(job)
                    .build();
            try {
                StdSchedulerFactory.getDefaultScheduler().scheduleJob(job, trigger);
            } catch (SchedulerException ex) {
                Logger.error("Cannot schedule cleanup for storage %s : %[exception]s", storage.identifier(), ex);
            }
        }
    }

}
