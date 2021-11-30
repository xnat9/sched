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
 * 定时任务调度器: 封装quartz
 */
public class Sched {
    protected static final Logger log = LoggerFactory.getLogger(Sched.class);
    private static final String KEY_FN = "fn";
    protected Scheduler scheduler;
    protected final Map<String, Object> attrs;
    protected final ExecutorService exec;


    public Sched() {this(null, null);}

    public Sched(Map<String, Object> attrs, ExecutorService exec) {
        this.attrs = attrs == null ? new ConcurrentHashMap<>() : attrs;
        this.exec = exec == null ? Executors.newFixedThreadPool(4, new ThreadFactory() {
            final AtomicInteger i = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "sched-" + i.getAndIncrement());
            }
        }) : exec;
    }


    /**
     * 初始化Quartz
     * @return {@link Sched}
     */
    public Sched init() {
        if (scheduler != null) throw new RuntimeException("Sched already inited");
        try {
            StdSchedulerFactory f = new StdSchedulerFactory();
            Properties p = new Properties(); p.putAll(attrs);
            p.setProperty(StdSchedulerFactory.PROP_THREAD_POOL_CLASS, AgentThreadPool.class.getName());
            f.initialize(p);
            AgentThreadPool.exec = exec;
            scheduler = f.getScheduler();
            scheduler.start();
            log.debug("Started sched(Quartz)");
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
     * 添加 时间表达式 任务
     * @param cron 时间表达式
     * @param fn 要执行的函数
     * @return {@link Sched}
     */
    public Sched cron(String cron, Runnable fn) {
        if (scheduler == null) throw new RuntimeException("Please init first");
        if (cron == null || cron.isEmpty()) throw new IllegalArgumentException("Param cron not empty");
        if (fn == null) throw new IllegalArgumentException("Param fn required");
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
        return this;
    }


    /**
     * 在将来的某个时间点执行
     * @param time 时间点
     * @param fn 任务函数
     * @return {@link Sched}
     */
    public Sched time(Date time, Runnable fn) {
        if (scheduler == null) throw new RuntimeException("Please init first");
        if (time == null) throw new IllegalArgumentException("Param time required");
        if (fn == null) throw new IllegalArgumentException("Param fn required");
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
            log.debug("add time '{}' job will execute at '{}'", id, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(d));
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
        return this;
    }


    /**
     * 在多少时间之后执行
     * @param duration 时间段
     * @param fn 任务函数
     * @return {@link Sched}
     */
    public Sched after(Duration duration, Runnable fn) {
        if (scheduler == null) throw new RuntimeException("Please init first");
        if (duration == null) throw new IllegalArgumentException("Param duration required");
        return time(new Date(duration.toMillis() + System.currentTimeMillis()), fn);
    }


    /**
     * 任务间隔执行
     * {@link #fixedDelay}: 每次任务执行完成后才开始计算下次执行时间
     * {@link #fixedRate}: 每次执行即计算下次执行时间. 注意函数自己应该捕获异常
     * @param duration 间隔时间
     * @param initialDelay 第一次延迟多久后执行, 为空则立即执行
     * @param fn 任务函数
     * @return {@link Sched}
     */
    public Sched fixedDelay(Duration duration, Duration initialDelay, Runnable fn) {
        if (scheduler == null) throw new RuntimeException("Please init first");
        if (duration == null) throw new IllegalArgumentException("Param duration required");
        if (fn == null) throw new IllegalArgumentException("Param fn required");
        final Runnable wrapFn = new Runnable() {
            @Override
            public void run() {
                try {
                    fn.run();
                } finally {
                    after(duration, this);
                }
            }
        };
        if (initialDelay == null) wrapFn.run();
        else after(initialDelay, wrapFn);
        return this;
    }

    /**
     * 任务间隔执行
     * @param duration 间隔时间
     * @param fn 任务函数
     * @return {@link Sched}
     */
    public Sched fixedDelay(Duration duration, Runnable fn) {
        return fixedDelay(duration, null, fn);
    }


    /**
     * 时间间隔执行
     * {@link #fixedDelay}: 每次任务执行完成后才开始计算下次执行时间
     * {@link #fixedRate}: 每次执行即计算下次执行时间. 注意函数自己应该捕获异常
     * @param duration 间隔时间
     * @param initialDelay 第一次延迟多久后执行, 为空则立即执行
     * @param fn 任务函数
     * @return {@link Sched}
     */
    public Sched fixedRate(Duration duration, Duration initialDelay, Runnable fn) {
        if (scheduler == null) throw new RuntimeException("Please init first");
        if (duration == null) throw new IllegalArgumentException("Param duration required");
        if (fn == null) throw new IllegalArgumentException("Param fn required");
        final Runnable wrapFn = new Runnable() {
            @Override
            public void run() {
                try {
                    exec.execute(fn);
                } finally {
                    after(duration, this);
                }
            }
        };
        if (initialDelay == null) wrapFn.run();
        else after(initialDelay, wrapFn);
        return this;
    }

    /**
     * 时间间隔执行
     * @param duration 间隔时间
     * @param fn 任务函数
     * @return {@link Sched}
     */
    public Sched fixedRate(Duration duration, Runnable fn) {
        return fixedRate(duration, null, fn);
    }


    /**
     * 动态任务调度执行. 自定义下次执行时间
     * @param dateSupplier 下次触发时间计算函数. 函数返回下次触发时间. 如果返回空 则停止
     * @param fn 任务函数
     * @return {@link Sched}
     */
    public Sched dyn(Supplier<Date> dateSupplier, Runnable fn) {
        if (scheduler == null) throw new RuntimeException("Please init first");
        if (dateSupplier == null) throw new IllegalArgumentException("Param dateSupplier required");
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
        return this;
    }


    /**
     * Quartz Scheduler
     * @return {@link Scheduler}
     */
    public Scheduler getScheduler() { return scheduler; }


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
