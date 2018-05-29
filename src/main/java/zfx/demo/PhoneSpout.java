package zfx.demo;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * spout组件的topo源组件
 */
public class PhoneSpout extends BaseRichSpout {
    public  String[] terminalPhones=new String[]{"apple","xiaomi","huawei","sumsung","moto"};
    private SpoutOutputCollector spoutOutputCollector;

    /**
     * 组件初始化方法  类似于mapreduce里面的setup方法
     */
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
    }

    /**
     * 消息的处理方法，不断往后续流程发送消息，调用一次发送一次tuple,会不断被workexcutor调用
     */
    public void nextTuple() {
        //模拟从外部数据源获取数据
        int index=new Random().nextInt(5);
        String phone=terminalPhones[index];
        //将拿到的数据封装为tuple发送出去
        spoutOutputCollector.emit(new Values(phone));
    }

    /**
     *  声明输出消息的字段，有几个字段，每个字段的字段名称
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //为发出去的tuple中定义字段名字
        outputFieldsDeclarer.declare(new Fields("phone"));

    }
}
