package cn.xnatural.sched;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 定时间任务调度器
 */
class SchedCus {
    protected static final Logger              log           = LoggerFactory.getLogger(SchedCus.class);
    protected final        Map<String, Object> attrs;
    protected final        ExecutorService     exec;
    protected final        Queue<Scheder>      scheders      = new ConcurrentLinkedQueue<>();
    protected final Object sigLock = new Object();
    protected final AtomicBoolean stop = new AtomicBoolean(false);
    protected final        Thread              triggerThread = new Thread(() -> {
        try {
            trigger();
        } catch (Throwable ex) {
            log.error("", ex);
        }
    }, "sched");


    public SchedCus() {this(null, null);}

    public SchedCus(Map<String, Object> attrs, ExecutorService exec) {
        this.attrs = attrs == null ? new ConcurrentHashMap<>() : attrs;
        this.exec = exec == null ? Executors.newFixedThreadPool(4, new ThreadFactory() {
            AtomicInteger i = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "sched-" + i.getAndIncrement());
            }
        }) : exec;
        triggerThread.start();
    }


    public void stop() {
        if (stop.compareAndSet(false, true)) {
            exec.shutdown();
        }
    }


    /**
     * cron 时间表达式
     * @param cron 时间表达式
     * @param fn 任务函数
     */
    public void cron(String cron, Runnable fn) {
        try {
            CronExpression expression = new CronExpression(cron);
            Scheder scheder = new Scheder();
            scheder.fn = fn;
            scheder.dateSupplier = (d) -> expression.getNextValidTimeAfter(d);
            scheders.offer(scheder);
            synchronized (sigLock) { sigLock.notify(); }
            log.info("add cron '{}' job", cron);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 在多少时间之后执行
     * @param duration 时间段. >=500ms
     * @param fn 任务函数
     */
    public void after(Duration duration, Runnable fn) {
        if (duration == null || fn == null) return;
        if (duration.toMillis() < getRange()) throw new IllegalArgumentException("duration must >=" + getRange() + "ms");
        Scheder scheder = new Scheder();
        scheder.fn = fn;
        Date time = new Date(System.currentTimeMillis() + duration.toMillis());
        scheder.dateSupplier = (date) -> time.getTime() > date.getTime() ? time : null;
        scheders.offer(scheder);
        synchronized (sigLock) { sigLock.notify(); }
        log.debug("add after '{}ms' job will execute at '{}'", duration.toMillis(), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(time));
    }


    /**
     * 在将来的某个时间点执行
     * @param time 时间点. >= 当前时间 + 500ms
     * @param fn 任务函数
     */
    public void time(Date time, Runnable fn) {
        if (time == null || fn == null) return;
        if (time.getTime() + getRange() < System.currentTimeMillis()) throw new IllegalArgumentException("time must >= (" + getRange() + "ms + 当前时间)");
        Scheder scheder = new Scheder();
        scheder.fn = fn;
        scheder.dateSupplier = (date) -> time.getTime() > date.getTime() ? time : null;
        scheders.offer(scheder);
        synchronized (sigLock) { sigLock.notify(); }
        log.debug("add time job will execute at '{}'", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(time));
    }


    /**
     * 动态任务调度执行. 自定义下次执行时间
     * @param dateSupplier 下次触发时间计算函数. 函数返回下次触发时间. 如果返回空 则停止
     * @param fn 任务函数
     */
    public void dyn(Supplier<Date> dateSupplier, Runnable fn) {
        if (dateSupplier == null || fn == null) return;
        Scheder scheder = new Scheder();
        scheder.fn = fn;
        scheder.dateSupplier = date -> dateSupplier.get();
        scheders.offer(scheder);
        synchronized (sigLock) { sigLock.notify(); }
        log.debug("add dyn job");
    }


    /**
     * 只能被执行一次
     */
    private final void trigger() {
        while (!stop.get()) {
            Date now = new Date();
            Date nextTriggerTime = null;
            for (Iterator<Scheder> it = scheders.iterator(); it.hasNext(); ) {
                Scheder scheder = it.next();
                scheder.tryRun(now);
                Date nextDate = scheder.nextDate(now);
                log.info("nextDate: " + nextDate);
                if (nextDate == null) {
                    it.remove(); continue;
                }
                nextTriggerTime = (nextTriggerTime == null || nextTriggerTime.getTime() > nextDate.getTime()) ? nextDate : nextTriggerTime;
            }
            try {
                synchronized (sigLock) {
                    sigLock.wait(
                            (nextTriggerTime == null ? Long.valueOf(attrs.getOrDefault("waitTimeout", 1000L * 60).toString()) : nextTriggerTime.getTime() - now.getTime())
                    );
                }
            } catch (InterruptedException e) {
                log.error("", e);
            }
        }
    }


    /**
     * 容错. 这个时间范围内的时间认为是相同时间
     * @return
     */
    public long getRange() {
        return Long.valueOf(attrs.getOrDefault("range", 500L).toString());
    }


    /**
     * 执行时间超时
     * @return
     */
    public long getTimeout() {
        return Long.valueOf(attrs.getOrDefault("timeout", 1000L * 60 * 5).toString());
    }


    /**
     * 定时任务
     */
    protected class Scheder {
        // 执行时间提供函数
        protected Function<Date, Date> dateSupplier;
        // 任务执行函数
        protected Runnable       fn;
        // 执行的记录
        protected Queue<Record>    dateQueue = new ConcurrentLinkedQueue<>();

        // 执行记录
        class Record {
            Date date; boolean down;
            public Record(Date date, boolean down) {
                this.date = date;
                this.down = down;
            }
        }


        /**
         * 判断是否到了执行的时候并执行
         */
        protected boolean tryRun(Date now) {
            if (dateQueue.isEmpty()) return false;
            Record record = dateQueue.peek();
            if (record == null) {
                nextDate(now);
                tryRun(now);
            }
            long gap = now.getTime() - record.date.getTime();
            if (gap > getTimeout()) { // 过了执行时间 并且超过误差范围
                dateQueue.remove(record);
                return false;
            }
            if (gap > 0 && gap <= getTimeout()) { //时间到了执行,在误差范围内
                dateQueue.remove(record);
                exec.execute(fn);
            }
            return true;
        }


        /**
         * 得到下次执行时间
         * @return
         */
        protected Date nextDate(Date now) {
            Record record = dateQueue.peek();
            if (record == null || record.down) {
                record = new Record(dateSupplier.apply(now), false);
                dateQueue.offer(record);
            }
            return record.date;
        }
    }
}
