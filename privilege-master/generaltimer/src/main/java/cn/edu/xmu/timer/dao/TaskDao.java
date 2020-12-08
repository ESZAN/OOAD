package cn.edu.xmu.timer.dao;

import cn.edu.xmu.ooad.util.ResponseCode;
import cn.edu.xmu.ooad.util.ReturnObject;
import cn.edu.xmu.timer.mapper.ParamPoMapper;
import cn.edu.xmu.timer.mapper.TaskPoMapper;
import cn.edu.xmu.timer.model.bo.Param;
import cn.edu.xmu.timer.model.bo.Task;
import cn.edu.xmu.timer.model.po.ParamPo;
import cn.edu.xmu.timer.model.po.ParamPoExample;
import cn.edu.xmu.timer.model.po.TaskPo;
import cn.edu.xmu.timer.model.po.TaskPoExample;
import cn.edu.xmu.timer.service.ScheduleJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/**
 * @Author Pinzhen Chen
 * @Date 2020/12/2 8:24
 */
@Repository
public class TaskDao {

//    @Value("${generaltimer.prepare-time}")
//    private Integer prepareTime;

    @Autowired
    private TaskPoMapper taskPoMapper;

    @Autowired
    private ParamPoMapper paramPoMapper;

    @Autowired
    private ScheduleJob scheduleJob;

    private static final Logger logger = LoggerFactory.getLogger(TaskDao.class);

    /**
     * 通过bo对象获取po对象
     * @author 24320182203173 Chen Pinzhen
     * @date: 2020/12/2 8:42
     */
    private TaskPo getTaskPoByTask(Task bo){
        TaskPo po=new TaskPo();
        po.setBeginTime(bo.getBeginTime());
        po.setReturnTypeName(bo.getReturnTypeName());
        po.setMethodName(bo.getMethodName());
        po.setTag(bo.getTag());
        if(bo.getTopic()==null)
            po.setTopic("default");
        else
            po.setTopic(bo.getTopic());
        po.setPeriod(bo.getPeriod());
        po.setBeanName(bo.getBeanName());
        return po;
    }

    /**
     * 通过po创建bo对象
     * @author 24320182203173 Chen Pinzhen
     * @date: 2020/12/2 8:49
     */
    private Task getTaskByTaskPo(TaskPo po){
        Task bo=new Task();
        bo.setId(po.getId());
        bo.setBeginTime(po.getBeginTime());
        bo.setGmtCreate(po.getGmtCreate());
        bo.setGmtModified(po.getGmtModified());
        bo.setTopic(po.getTopic());
        bo.setTag(po.getTag());
        bo.setReturnTypeName(po.getReturnTypeName());
        bo.setMethodName(po.getMethodName());
        bo.setPeriod(po.getPeriod());
        bo.setBeanName(po.getBeanName());
        //计算sendTime
//        LocalDateTime sendTime=po.getBeginTime().minusSeconds(prepareTime);
//        bo.setSendTime(sendTime);
        if(po.getTopic().equals("default")){
            bo.setSenderName("localExecute");
            bo.setTopic(null);
        }
        else
            bo.setSenderName("remoteRocketMQExecute");
        return bo;
    }

    /**
         * 通过po创建bo对象
         * @param po
         * @return Param
         * @author 24320182203173 Chen Pinzhen
         * @date: 2020/12/3 8:39
         */
    private Param getParamByParamPo(ParamPo po){
        Param bo=new Param();
        bo.setId(po.getId());
        bo.setGmtCreate(po.getGmtCreate());
        bo.setGmtModified(po.getGmtModified());
        bo.setParamValue(po.getParamValue());
        bo.setSeq(po.getSeq());
        bo.setTypeName(po.getTypeName());
        return bo;
    }

    /**
         * 通过bo创建bo对象
         * @param bo
         * @return ParamPo
         * @author 24320182203173 Chen Pinzhen
         * @date: 2020/12/3 8:48
         */
    private ParamPo getParamPoByParam(Param bo){
        ParamPo po=new ParamPo();
        po.setParamValue(bo.getParamValue());
        po.setSeq(bo.getSeq());
        po.setTypeName(bo.getTypeName());
        return po;
    }

    /**
         * timer001: 创建定时任务
         * @param task
         * @param period
         * @return ReturnObject<Task>
         * @author 24320182203173 Chen Pinzhen
         * @date: 2020/12/2 8:39
         */
    @Transactional
    public ReturnObject<Task> createTask(Task task, Integer period) {
        task.setPeriod(period.byteValue());
        TaskPo taskPo = getTaskPoByTask(task);
        taskPo.setPeriod(period.byteValue());
        ReturnObject<Task> retObj = null;
        try{
            int ret = taskPoMapper.insertSelective(taskPo);
            if (ret == 0) {
                //插入失败
                logger.debug("insertTask: insert task fail " + taskPo.toString());
                retObj = new ReturnObject<>(ResponseCode.RESOURCE_ID_NOTEXIST, String.format("新增失败：" + taskPo.getBeanName()));
            } else {
                //插入taskPo成功
                logger.debug("insertTask: insert task = " + taskPo.toString());
                Task taskRet = getTaskByTaskPo(taskPo);
                taskRet.setGmtCreate(LocalDateTime.now());
                List<Param> paramList=task.getParamList();
                if(paramList!=null && !paramList.isEmpty()){
                    //把task中每个参数插入param表中
                    List<Param> retParamList = new ArrayList<>();
                    for (Param param : paramList) {
                        ParamPo paramPo=getParamPoByParam(param);
                        //为paramPo设置当前的taskId
                        paramPo.setTaskId(taskRet.getId());
                        paramPoMapper.insertSelective(paramPo);
                        //插入paramPo成功
                        Param retParam = getParamByParamPo(paramPo);
                        retParam.setGmtCreate(LocalDateTime.now());
                        retParamList.add(retParam);
                    }
                    //按照seq升序排序
                    retParamList.sort(Comparator.comparing(Param::getSeq));
                    taskRet.setParamList(new ArrayList<>(retParamList));
                }
//                List<Task> retList=new ArrayList<>();
//                retList.add(taskRet);
//                //将新建任务放入时间轮
//                scheduleJob.addJob(retList);
                retObj = new ReturnObject<>(taskRet);
            }
        }
        catch (DataAccessException e) {
            // 其他数据库错误
            logger.debug("other sql exception : " + e.getMessage());
            retObj = new ReturnObject<>(ResponseCode.INTERNAL_SERVER_ERR, String.format("数据库错误：%s", e.getMessage()));
        }
        catch (Exception e) {
            // 其他Exception错误
            logger.error("other exception : " + e.getMessage());
            retObj = new ReturnObject<>(ResponseCode.INTERNAL_SERVER_ERR, String.format("发生了严重的数据库错误：%s", e.getMessage()));
        }
        return retObj;
    }


    /**
         * timer001: 获取topic下的所有定时任务
         * @param topic
         * @param tag
         * @return ReturnObject<List<Task>>
         * @author 24320182203173 Chen Pinzhen
         * @date: 2020/12/2 8:52
         */
    public ReturnObject<List<Task>> getTaskByTopic(String topic, String tag) {
        TaskPoExample taskPoExample=new TaskPoExample();
        TaskPoExample.Criteria criteria=taskPoExample.createCriteria();
        criteria.andTopicEqualTo(topic);
        if(tag!=null)
            criteria.andTagEqualTo(tag);
        ReturnObject<List<Task>> retObj = null;
        try{
            List<TaskPo> taskPos = taskPoMapper.selectByExample(taskPoExample);
                logger.debug("selectTaskList: select Task = " + taskPos.toString());
                List<Task> taskList=new ArrayList<>();
            for (TaskPo po : taskPos) {
                Task task = getTaskByTaskPo(po);
                //根据taskid找到param
                ParamPoExample paramPoExample=new ParamPoExample();
                ParamPoExample.Criteria criteria1=paramPoExample.createCriteria();
                criteria1.andTaskIdEqualTo(po.getId());
                List<ParamPo> paramPos=paramPoMapper.selectByExample(paramPoExample);
                List<Param> paramList=new ArrayList<>();
                for (ParamPo paramPo : paramPos) {
                    Param param=getParamByParamPo(paramPo);
                    paramList.add(param);
                }
                //加入参数列表
                task.setParamList(new ArrayList<>(paramList));
                taskList.add(task);
            }
                retObj = new ReturnObject<>(taskList);
        }
        catch (DataAccessException e) {
                // 数据库错误
                logger.debug("other sql exception : " + e.getMessage());
                retObj = new ReturnObject<>(ResponseCode.INTERNAL_SERVER_ERR, String.format("数据库错误：%s", e.getMessage()));
        }
        catch (Exception e) {
            // 其他Exception错误
            logger.error("other exception : " + e.getMessage());
            retObj = new ReturnObject<>(ResponseCode.INTERNAL_SERVER_ERR, String.format("发生了严重的数据库错误：%s", e.getMessage()));
        }
        return retObj;
    }

    @Transactional
    public ReturnObject removeTask(Long id){
        try {
            ParamPoExample example=new ParamPoExample();
            ParamPoExample.Criteria criteria=example.createCriteria();
            criteria.andTaskIdEqualTo(id);
            paramPoMapper.deleteByExample(example);
            taskPoMapper.deleteByPrimaryKey(id);
            return new ReturnObject();
        }
        catch (DataAccessException e){
            logger.error("removeTask: DataAccessException:" + e.getMessage());
            return new ReturnObject<>(ResponseCode.INTERNAL_SERVER_ERR, String.format("数据库错误：%s", e.getMessage()));
        }
        catch (Exception e) {
            // 其他Exception错误
            logger.error("otherError: DataAccessException:" + e.getMessage());
            return new ReturnObject<>(ResponseCode.INTERNAL_SERVER_ERR, String.format("发生了严重的数据库错误：%s", e.getMessage()));
        }
    }

    @Transactional
    public ReturnObject cleanTaskByTopic(String topic, String tag){
        try {
            List<Long> list = new ArrayList<>();
            TaskPoExample example=new TaskPoExample();
            TaskPoExample.Criteria criteria=example.createCriteria();
            criteria.andTopicEqualTo(topic);
            if(!Objects.equals(null,tag))
            {
                criteria.andTagEqualTo(tag);
            }
            List<TaskPo> taskPos=taskPoMapper.selectByExample(example);
            for(TaskPo po:taskPos)
            {
                if(Objects.equals(po.getPeriod(),(byte) 8))
                {
                    list.add(po.getId());
                }
                else if(Objects.equals(po.getPeriod(),(byte) 0))
                {
                    LocalDate localDate=LocalDate.of(po.getBeginTime().getYear(),po.getBeginTime().getMonth(),po.getBeginTime().getDayOfMonth());
                    if(Objects.equals(localDate,LocalDate.now()) || Objects.equals(localDate,LocalDate.now().plusDays(1)))
                    {
                        list.add(po.getId());
                    }
                }
                else if( Objects.equals(po.getPeriod(),(byte)10))
                {
                    LocalDateTime localDateTime=po.getBeginTime();
                    if(Objects.equals(localDateTime.getMonth(),LocalDateTime.now().getMonth()) && Objects.equals(localDateTime.getDayOfMonth(),LocalDateTime.now().getDayOfMonth()))
                    {
                        list.add(po.getId());
                    }
                    if(Objects.equals(localDateTime.getMonth(),LocalDateTime.now().plusDays(1).getMonth()) && Objects.equals(localDateTime.getDayOfMonth(),LocalDateTime.now().plusDays(1).getDayOfMonth()))
                    {
                        list.add(po.getId());
                    }
                }
                else if(Objects.equals(po.getPeriod(),(byte)9))
                {
                    int day=po.getBeginTime().getDayOfMonth();
                    int day1=LocalDateTime.now().getDayOfMonth();
                    int day2=LocalDateTime.now().plusDays(1).getDayOfMonth();
                    if(day==day1 || day==day2)
                    {
                        list.add(po.getId());
                    }
                }
                else {
                    DayOfWeek day = po.getBeginTime().getDayOfWeek();
                    DayOfWeek day1 = LocalDateTime.now().getDayOfWeek();
                    DayOfWeek day2 = LocalDateTime.now().plusDays(1).getDayOfWeek();
                    if(day==day1 || day==day2)
                    {
                        list.add(po.getId());
                    }
                }
                ParamPoExample example1=new ParamPoExample();
                ParamPoExample.Criteria criteria1=example1.createCriteria();
                criteria1.andTaskIdEqualTo(po.getId());
                paramPoMapper.deleteByExample(example1);
                taskPoMapper.deleteByPrimaryKey(po.getId());
            }
            return new ReturnObject<>(list);
        }
        catch (DataAccessException e){
            logger.error("cleanTaskByTopic: DataAccessException:" + e.getMessage());
            return new ReturnObject<>(ResponseCode.INTERNAL_SERVER_ERR, String.format("数据库错误：%s", e.getMessage()));
        }
        catch (Exception e) {
            // 其他Exception错误
            logger.error("otherError: DataAccessException:" + e.getMessage());
            return new ReturnObject<>(ResponseCode.INTERNAL_SERVER_ERR, String.format("发生了严重的数据库错误：%s", e.getMessage()));
        }
    }

}
