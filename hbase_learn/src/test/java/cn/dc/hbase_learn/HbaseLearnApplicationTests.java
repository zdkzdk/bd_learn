package cn.dc.hbase_learn;


import cn.dc.hbase_learn.utils.HbaseTemplateUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;

import java.io.IOException;
import java.util.List;

@SpringBootTest
class HbaseLearnApplicationTests {

    @Autowired
    Configuration conf;
    @Autowired
    HbaseTemplate template;
    Connection connection;
    @Autowired
    HbaseTemplateUtil hbaseTemplateUtil;

    HBaseAdmin admin;//操作hbase的对象
    HTable hTable;//操作表的对象
    TableName tableName;

    @BeforeEach
    public void init() throws IOException {
        tableName = TableName.valueOf("11");
        connection = hbaseTemplateUtil.getConn();
        admin = (HBaseAdmin) connection.getAdmin();
        // admin = new HBaseAdmin(conf);
        hTable = (HTable)connection.getTable(tableName);
        //hTable = new HTable(conf, tableName);
    }

    @Test
    void createTable() throws IOException {
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        //表信息
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        //列族信息,要放到表信息中
        HColumnDescriptor cf = new HColumnDescriptor("cf".getBytes());
        descriptor.addFamily(cf);
        admin.createTable(descriptor);
    }
    @Test
    void createTableByTemplate() throws IOException {
        hbaseTemplateUtil.createTable("asd", "cff");
    }

    @Test
    void testTemplate() {
        template.put("qwe", "qqq", "cf", "qqq", "值".getBytes());
        String res = this.getColumn("qwe", "qqq", "cf", "qqq");
        System.out.println(res);
    }

    public String getColumn(String tableName, String rowkey, String family, String column) {
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(family)
                || StringUtils.isBlank(rowkey) || StringUtils.isBlank(column)) {
            return null;
        }
        return template.get(tableName, rowkey, family, column, new RowMapper<String>() {
            public String mapRow(Result result, int rowNum) throws Exception {
                List<Cell> ceList = result.listCells();
                String res = "";
                if (ceList != null && ceList.size() > 0) {
                    for (Cell cell : ceList) {
                        res = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    }
                }
                return res;
            }
        });
    }
}
