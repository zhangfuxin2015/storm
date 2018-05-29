package zfx.demo;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * 添加日期后缀，然后输出结果到外部
 */
public class PhoneSuffixBolt extends BaseBasicBolt {
    private  FileWriter fileWriter;
    /**
     * bolt的初始化方法，实在bolt组件实例化的时候调用一次
     * @param stormConf
     * @param context
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            fileWriter=new FileWriter("/Users/zhangfuxin/Desktop/testPhone/"+ UUID.randomUUID());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String phone=tuple.getString(0);
        String result=phone+"2020-12-20";
        //因为本bolt已经是top的最后一级，处理结果输出到外部存储系统中。
        try {
            fileWriter.write(result+"\n");
            fileWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *这个是最后一个bolt，不需要再往下一个组件发送消息，所以不需要定义
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
