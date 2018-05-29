package zfx.demo;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 将手机名称转为大写，不断被worker 的executor线程调用，没收到一个消息，tuple就调用一次，
 */
public class PhoneUpdateBolt extends BaseBasicBolt {
    /**
     *
     * @param tuple 上一次组件 发来的消息，
     * @param basicOutputCollector   用来发出消息的工具
     */
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //拿到要处理的数据
        // 根据tuple中field的名称来获取
        String phon1=tuple.getStringByField("phone");
        //获取方式二：根据要处理的value在tuple中的角标获取
        String phone2=tuple.getString(0);
        String phoneResult = phone2.toUpperCase();
        basicOutputCollector.emit(new Values(phoneResult));
    }

    /**
     * 声明这个组件发出消息的格式
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("phone"));
    }
}
