# 介绍
定时任务: 封装quartz

# 安装教程
```xml
<dependency>
    <groupId>cn.xnatural.sched</groupId>
    <artifactId>sched</artifactId>
    <version>1.0.2</version>
</dependency>
```

# 五种时间任务调度
```java
Sched sched = new Sched().init();
```

## cron时间表达式
> sched.cron(cron表达式, 任务函数)
```java
sched.cron("0 0/5 * * * ? ", () -> {
    System.out.println("每隔5分钟执行")
})
```

## 一段时间之后执行
> sched.after(一段时间(Duration), 任务函数)
```java
sched.after(Duration.ofMinutes(3), () -> {
    System.out.println("3分钟之后执行")
});
```

## 在将来的某个时间点执行
> sched.time(时间点(Date), 任务函数)
```java
sched.time(
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2020-12-12 11:55:33"), 
    () -> System.out.println("2020-12-12 11:55:33 执行")
);
```

## 动态任务调度执行. 自定义下次执行时间
> sched.dyn(时间函数(返回Date), 任务函数)
```java
sched.dyn(() -> { //每次执行完任务函数,会获取一次下次执行时间
    if (new Random().nextInt(100) == 70) return null; // 返回null任务停止
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.MINUTE, new Random().nextInt(30) + 10);
    return cal.getTime();
}, () -> System.out.println("动态任务执行"));
```

## 任务间隔执行
> sched.fixedDelay(时间间隔(Duration), 任务函数)
```java
sched.fixedDelay(Duration.ofSeconds(2), () -> {
    System.out.println("fixedDelay====");
});
```

# 参与贡献
xnatural@msn.cn
