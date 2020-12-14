import cn.xnatural.sched.Sched;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

public class SchedTest {
    static final Logger log = LoggerFactory.getLogger(SchedTest.class);

    public static void main(String[] args) throws Exception {
        // SchedQuartz sched = new SchedQuartz();
        Sched sched = new Sched();
        sched.cron("0/10 * * * * ? ", () -> log.info("cron 每隔10秒钟执行"));
        sched.after(Duration.ofSeconds(15), () -> log.info("after 15秒之后执行"));
        Date d = new Date(System.currentTimeMillis() + Duration.ofSeconds(10).toMillis());
        sched.time(d, () -> log.info("time " + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(d) + " 执行")));
        sched.dyn(() -> {
            if (new Random().nextInt(20) == 17) return null; // 返回null任务停止
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.SECOND, new Random().nextInt(10) + 10);
            Date date = cal.getTime();
            log.info("dyn 获取时间: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(date));
            return date;
        }, () -> log.info("dyn 任务执行: "  + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(new Date())));

        Thread.sleep(1000 * 60 * 2);
        sched.stop();
        log.error("end =================");
    }
}
