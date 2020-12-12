package cn.xnatural.sched;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

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
            for (Map.Entry<String, CronExpression> entry : crons.entrySet()) {

            }
        }
    }
}
