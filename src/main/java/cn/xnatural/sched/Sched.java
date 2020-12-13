package cn.xnatural.sched;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class Sched {
    protected static final Logger log = LoggerFactory.getLogger(Sched.class);
    protected final Map<String, Object> attrs;
    protected final ExecutorService exec;
    protected final Map<String, CronExpression> crons = new ConcurrentHashMap<>();
    protected final Map<String, Runnable> tasks = new ConcurrentHashMap<>();
    protected final Thread thread = new Thread(() -> {
        try {
            trigger();
        } catch (Throwable ex) {
            log.error("", ex);
        }
    }, "sched");


    public Sched() {this(null, null);}

    public Sched(Map<String, Object> attrs, ExecutorService exec) {
        this.attrs = attrs == null ? new ConcurrentHashMap<>() : attrs;
        this.exec = exec == null ? Executors.newFixedThreadPool(4, new ThreadFactory() {
            AtomicInteger i = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "sched-" + i.getAndIncrement());
            }
        }) : exec;
        thread.start();
    }


    /**
     * cron 时间表达式
     * @param cron 时间表达式
     * @param fn 任务函数
     */
    public void cron(String cron, Runnable fn) {
        try {
            CronExpression expression = new CronExpression(cron);
            crons.put(cron, expression);
            tasks.put(cron, fn);
            trigger();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }


    protected void trigger() {
        while (true) {
            for (Iterator<Map.Entry<String, CronExpression>> it = crons.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, CronExpression> entry = it.next();
                // entry.getValue().getne
            }
        }
    }


    protected class Scheder {
        protected CronExpression expression;
        protected Supplier<Date> dateSupplier;
        // protected final LinkedHashMap<Long, Boolean> history = new LinkedHashMap<>();
        Date date;
        boolean down;// date 这个时间是否已执行
        //时间误差. 默认 5秒
        protected Duration range = Duration.ofSeconds(5);

        protected void run() {
            if (date == null) return;
            if (down) return;
            long gap = System.currentTimeMillis() - date.getTime();
            if (gap > range.toMillis()) { // 过了执行时间 并且超过误差范围
                date = null;
            }
            if (Math.abs(gap) < range.toMillis()) { //时间到了执行

            }
        }

        protected Date nextDate() { //得到下次执行时间
            if (down || date == null) {
                date = dateSupplier.get();
            }
            return date;
        }
    }
}
