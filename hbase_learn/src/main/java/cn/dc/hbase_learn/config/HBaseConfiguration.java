package cn.dc.hbase_learn.config;

import cn.dc.hbase_learn.utils.HbaseTemplateUtil;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

@org.springframework.context.annotation.Configuration
public class HBaseConfiguration {

    @Value("${hbase.zookeeper.quorum}")
    private String zookeeperQuorum;

    @Value("${hbase.zookeeper.property.clientPort}")
    private String clientPort;

    @Value("${zookeeper.znode.parent}")
    private String znodeParent;


    @Bean
    public Configuration getConf() {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        conf.set("hbase.zookeeper.property.clientPort", clientPort);
        //conf.set("zookeeper.znode.parent", znodeParent);

        return conf;
    }

    @Bean
    public HbaseTemplate getHbaseTemplate(@Autowired Configuration conf) {
        return new HbaseTemplate(conf);
    }

    @Bean
    public HbaseTemplateUtil getHbaseTemplateUtil(){
        return new HbaseTemplateUtil();
    }
}

