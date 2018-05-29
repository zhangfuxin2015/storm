package zfx.demo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 构造一个topology,并向storm集群提交
 */
public class TopSubmit {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        //先获得一个topplogy 构建器
        TopologyBuilder topologyBuilder= new TopologyBuilder();
        //制定top 用的spout的参数 参数， spout的id（自定义）， spout 的实例对象
        topologyBuilder.setSpout("phoneSpout",new PhoneSpout());
        //指定top所用的bolt组件,从phoneSpout 留过来的数据，随机 分配
        topologyBuilder.setBolt("phoneUpdate",new PhoneUpdateBolt()).shuffleGrouping("phoneSpout");
        //指定第二个blot
        topologyBuilder.setBolt("suffix-bolt",new PhoneSuffixBolt()).shuffleGrouping("phoneUpdate");
        //使用build 创建toplogy对象
        StormTopology topology = topologyBuilder.createTopology();
        Config config=new  Config();
        config.setNumWorkers(4);
        //将phone top提交集群运行。
        //StormSubmitter.submitTopology("phone-top",config,topology);
        //启动后会一直运行，可以storm kill phone-top 来停止程序

        //storm 支持本地模拟测试
        LocalCluster localCluster=new LocalCluster();
        localCluster.submitTopology("phone-top",config,topology);
    }
}
