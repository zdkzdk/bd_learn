package cn.dc.hbase_learn;

import ch.qos.logback.classic.db.names.TableName;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

import java.io.IOException;

@SpringBootApplication
public class HbaseLearnApplication {



    public static void main(String[] args) {
        SpringApplication.run(HbaseLearnApplication.class, args);
    }

}
