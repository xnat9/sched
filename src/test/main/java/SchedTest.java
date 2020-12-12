import cn.xnatural.sched.SchedQuartz;
import org.quartz.CronExpression;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

public class SchedTest {

    public static void main(String[] args) throws Exception {
        SchedQuartz sched = new SchedQuartz();
        sched.cron("0 0/5 * * * ? ", () -> System.out.println("每隔5分钟执行"));
        sched.after(Duration.ofMinutes(3), () -> System.out.println("3分钟之后执行"));
        sched.time(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2020-12-12 11:55:33"), () -> System.out.println("2020-12-12 11:55:33 执行"));
        sched.dyn(() -> {
            if (new Random().nextInt(100) == 70) return null; // 返回null任务停止
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.MINUTE, new Random().nextInt(30) + 10);
            return cal.getTime();
        }, () -> System.out.println("动态任务执行"));
    }
}
