package cn.edu.xmu.timer.service;

import cn.edu.xmu.ooad.util.ReturnObject;
import cn.edu.xmu.timer.client.TaskFactory;
import cn.edu.xmu.timer.model.bo.TaskMessage;
import cn.edu.xmu.timer.service.impl.TimeServiceImpl;
import cn.edu.xmu.timer.util.ExecuteFactory;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import cn.edu.xmu.timer.model.bo.Param;
import cn.edu.xmu.timer.model.bo.Task;
import cn.edu.xmu.timer.model.bo.TimeWheel;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


import javax.annotation.Resource;
import java.lang.reflect.InvocationTargetException;

import java.lang.reflect.Array;
import java.time.Duration;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

/**
 * @author Ming Qiu
 * @date Created in 2020/12/1 20:57
 **/
@Service
@Slf4j
public class ScheduleJob implements InitializingBean {
    @Autowired
    private TaskFactory taskFactory;

    @Autowired
    private ExecuteFactory executeFactory;

    @Value("${generaltimer.prepare-time}")
    private Integer prepareTime;

    /**
     * 小时轮，每个格子30分钟
     */
    private TimeWheel hourWheel;

    /**
     * 分钟轮，每个格子30秒
     */
    private TimeWheel minuteWheel;

    /**
     * 秒轮，每个格子1秒
     */
    private TimeWheel secondWheel;

    /**
     * timer009: 启动初始化
     * 按照启动时间，把12小时的内容装载入时间轮
     * @return
     */
    @Override
    public void afterPropertiesSet() throws Exception {

        this.hourWheel = new TimeWheel(48, 0);

        this.minuteWheel = new TimeWheel(120, 0);

        this.secondWheel = new TimeWheel(60, 0);

        System.out.println("afterPropertiesSet: "+minuteWheel.toString());
    }

    /**
     * timer003: 将下一半天的任务装载到小时轮上(注意提前量)
     *  1. 小时周期任务需要在每个小时重复放
     *  2. 要把下一次的半天装载任务定义在小时格中，放在第0，1，2，3，4，5个小时中任务最少的完成
     * @param beginTime 起
     * @param endTime 止
     * @return
     */
    @Async("jobExecutor")
    public void loadNextDayTask(LocalDateTime beginTime, LocalDateTime endTime){

    }

    /**
     * timer004: 将下一半小时的任务装载到分钟轮上
     * 1. 要把下一次的半小时装载任务定义在分钟格中，放在第0 - 14分钟中任务最少的完成
     * @return
     */
    @Async("jobExecutor")
    public void loadNextHourTask(){

    }

    /**
     * timer005: 将下一半分钟的任务装载到秒轮上
     * 1. 要把下一次的半分钟装载任务定义在秒格中，放在第0 - 14秒中任务最少的完成
     * @return
     */
    @Async("jobExecutor")
    public void loadNextMinuteTask(){
        //获取要装载的任务
        List<Task> loadTaskList = minuteWheel.nextCell();


        List<Task>[] lists = new List[30];

        //初始化
        for(int i = 0; i < 30; i++){
            lists[i] = new ArrayList<Task>();
        }

        //记录每个时间的任务数
        int[] taskNum = new int[15];

        for (Task task : loadTaskList) {
            int time = task.getBeginTime().getSecond() % 30;
            lists[time].add(task);
            if (time < 15) {
                taskNum[time]++;
            }
        }


        //记录任务数最少量
        int minNum = taskNum[0];
        //记录任务数最少的时刻
        int minRecord = 0;

        for(int i = 1; i < 15; i++){
            if(taskNum[i] < minNum) {
                minNum = taskNum[i];
                minRecord = i;
            }
        }

        Map<Integer,List<Task>> map = new TreeMap<>();
        for(int i = 0 ; i < 30; i++){
            map.put(i,lists[i]);
        }

        Task task = new Task();
        task.setBeanName(ScheduleJob.class.getName());
        task.setSenderName(ScheduleJob.class.getName());
        task.setMethodName("loadNextMinuteTask");
        task.setReturnTypeName("void");
        task.setTag(null);
        task.setTopic(null);
        task.setParamList(new ArrayList<Param>());
        LocalDateTime loadTime = loadTaskList.isEmpty()?LocalDateTime.now() : loadTaskList.get(0).getBeginTime();

        LocalDateTime localDateTime = LocalDateTime.of(loadTime.getYear(),loadTime.getMonth(),
                loadTime.getDayOfMonth(),loadTime.getHour(),loadTime.getMinute(),minRecord);
        task.setBeginTime(localDateTime);

        lists[minRecord].add(task);
        //加入下一次装载任务
        map.put(minRecord,lists[minRecord]);

        log.debug("loadNextMinuteTask thread: "+Thread.currentThread().toString());

        log.debug("loadNextMinuteTask map: " + map.toString());

        secondWheel.loadNextRound(map);

        log.debug("loadNextMinuteTask cells: " + this.getSecondWheel().getCells().toString());
    }

    /**
     * timer006: 每秒钟执行一次, 执行秒轮的任务
     * 1. 判断任务是本地任务还是远程任务， 本地任务topic=null，远程任务向消息队列发送消息
     */
    @Scheduled(cron = "*/1 * * * * ?")
    @Async("jobExecutor")
    public void execute() throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
        int second=LocalDateTime.now().getSecond();
            while(secondWheel.getCurrent()<=second){
                //1. 获取下一秒钟待执行任务
                List<Task> tasks=secondWheel.nextCell();

                //2. 根据任务类型决定该任务的执行方式
                for(Task t:tasks){
                    executeFactory.getExecuteWay(t).execute(t);
                }
            }


    }

    /**
     * timer007: 在时间轮里清除任务
     * @param taskIds 任务的id的列表
     * @return
     */
    public void removeJob(List<Long> taskIds){
        // 若传入 id 为空则认为不需要进行清除
        if (null == taskIds) {
            return;
        }

        if (null != hourWheel) {
            this.hourWheel  .removeTask(taskIds);
        }
        if (null != this.minuteWheel) {
            this.minuteWheel.removeTask(taskIds);
        }
        if (null != this.secondWheel) {
            this.secondWheel.removeTask(taskIds);
        }
    }

    /**
     * timer008: 在时间轮里增加任务
     * @param taskList 任务的的列表
     * @return
     */
    public void addJob(List<Task> taskList){

    }

    public TimeWheel getHourWheel() {
        return hourWheel;
    }

    public TimeWheel getMinuteWheel() {
        return minuteWheel;
    }

    public TimeWheel getSecondWheel() {
        return secondWheel;
    }

}
