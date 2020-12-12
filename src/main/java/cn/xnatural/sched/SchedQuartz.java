package cn.xnatural.sched;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.spi.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * 定时任务调度器
 */
public class SchedQuartz {
    protected static final Logger log = LoggerFactory.getLogger(SchedQuartz.class);
    private static final String KEY_FN = "fn";
    protected Scheduler scheduler;
    protected final Map<String, Object> attrs;
    protected final ExecutorService exec;


    public SchedQuartz() {this(null, null);}

    public SchedQuartz(Map<String, Object> attrs, ExecutorService exec) {
        this.attrs = attrs == null ? new ConcurrentHashMap<>() : attrs;
        this.exec = exec == null ? Executors.newFixedThreadPool(4, new ThreadFactory() {
            AtomicInteger i = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "sched-" + i.getAndIncrement());
            }
        }) : exec;
    }


    /**
     * 初始化Quartz
     * @return
     */
    public SchedQuartz start() {
        if (scheduler != null) throw new RuntimeException("Sched is already running");
        try {
            StdSchedulerFactory f = new StdSchedulerFactory();
            Properties p = new Properties(); p.putAll(attrs);
            p.setProperty(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, AgentThreadPool.class.getName());
            f.initialize(p);
            AgentThreadPool.exec = exec;
            scheduler = f.getScheduler();
            scheduler.start();
            log.info("Started sched(Quartz)");
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
        return this;
    }


    /**
     * 停止Quartz
     */
    public void stop() {
        log.debug("Shutdown 'sched'(Quartz) Server");
        try {
            scheduler.shutdown();
        } catch (SchedulerException e) {
            log.error("", e);
        }
        scheduler = null;
        AgentThreadPool.exec = null;
    }


    /**
     * cron 时间表达式
     * @param cron
     * @param fn
     */
    public void cron(String cron, Runnable fn) {
        if (scheduler == null) throw new RuntimeException("Sched is not running");
        if (cron == null || fn == null) throw new IllegalArgumentException("'cron' and 'fn' must not be empty");
        final JobDataMap data = new JobDataMap();
        data.put(KEY_FN, fn);
        String id = cron + "_" + System.currentTimeMillis();
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(new TriggerKey(id, "cron"))
                .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                .build();
        try {
            Date d = scheduler.scheduleJob(
                    JobBuilder.newJob(JopTpl.class).withIdentity(id, "cron").setJobData(data).build(),
                    trigger
            );
            log.info("add cron '{}' job will execute last time '{}'", id, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(d));
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 在多少时间之后执行
     * @param duration 时间段
     * @param fn 任务函数
     */
    public void after(Duration duration, Runnable fn) {
        if (scheduler == null) throw new RuntimeException("Sched is not running");
        if (duration == null || fn == null) throw new IllegalArgumentException("'duration' and 'fn' must not be null");
        final JobDataMap data = new JobDataMap();
        data.put(KEY_FN, fn);
        String id = duration.toMillis() + "_" + UUID.randomUUID().toString().replace("-", "");
        SimpleDateFormat sdf = new SimpleDateFormat("ss mm HH dd MM ? yyyy");
        String cron = sdf.format(new Date(new Date().getTime() + duration.toMillis()));
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(new TriggerKey(id, "after"))
                .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                .build();
        try {
            Date d = scheduler.scheduleJob(
                    JobBuilder.newJob(JopTpl.class).withIdentity(id, "after").setJobData(data).build(),
                    trigger
            );
            log.debug("add after '{}' job will execute at '{}'", id, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(d));
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 在将来的某个时间点执行
     * @param time 时间点
     * @param fn 任务函数
     */
    public void time(Date time, Runnable fn) {
        if (scheduler == null) throw new RuntimeException("Sched is not running");
        if (time == null || fn == null) throw new IllegalArgumentException("'time' and 'fn' must not be null");
        final JobDataMap data = new JobDataMap();
        data.put(KEY_FN, fn);
        String id = time + "_" + UUID.randomUUID().toString().replace("-", "");
        SimpleDateFormat sdf = new SimpleDateFormat("ss mm HH dd MM ? yyyy");
        String cron = sdf.format(time);
        Trigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(new TriggerKey(id, "time"))
                .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                .build();
        try {
            Date d = scheduler.scheduleJob(
                    JobBuilder.newJob(JopTpl.class).withIdentity(id, "time").setJobData(data).build(),
                    trigger
            );
            log.info("add time '{}' job will execute at '{}'", id, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(d));
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 动态任务调度执行. 自定义下次执行时间
     * @param dateSupplier
     *      下次触发时间计算函数. 函数返回下次触发时间. 如果返回空 则停止
     *      NOTE: 执行函数之前会计算下次执行的时间
     * @param fn 任务函数
     */
    public void dyn(Supplier<Date> dateSupplier, Runnable fn) {
        if (scheduler == null) throw new RuntimeException("Sched is not running");
        if (dateSupplier == null) throw new RuntimeException("dateSupplier must not be null");
        time(dateSupplier.get(), new Runnable() {
            @Override
            public void run() {
                try {
                    fn.run();
                } finally {
                    Date d = dateSupplier.get();
                    if (d != null) time(d, this);
                }
            }
        });
    }


    /**
     * Quartz job 模板类
     */
    public static class JopTpl implements Job {
        @Override
        public void execute(JobExecutionContext ctx) {
            ((Runnable) ctx.getMergedJobDataMap().get(KEY_FN)).run();
        }
    }


    /**
     * 代理线程池
     */
    public static class AgentThreadPool implements ThreadPool {
        static Executor exec;
        @Override
        public boolean runInThread(Runnable fn) {
            if (exec == null) fn.run();
            else exec.execute(fn);
            return true;
        }

        @Override
        public int blockForAvailableThreads() { return 1; } // 为1 就是每次 取一个距离时间最近的一个trigger org.quartz.simpl.RAMJobStore.acquireNextTriggers timeTriggers.first()

        @Override
        public void initialize() throws SchedulerConfigException { }

        @Override
        public void shutdown(boolean waitForJobsToComplete) { }

        @Override
        public int getPoolSize() { return -1; }

        @Override
        public void setInstanceId(String schedInstId) { }

        @Override
        public void setInstanceName(String schedName) { }
    }
}
