package cn.dc.zk_learn;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class ZKAPI {
    private static final int SESSION_TIMEOUT = 30000;

    public static final Logger LOGGER = LoggerFactory.getLogger(ZKAPI.class);

    private ZooKeeper zooKeeper;

    /**
     * 连接zookeeper
     * @throws IOException
     */
    @BeforeAll
    public void connect() throws IOException {
        zooKeeper = new ZooKeeper("192.168.9.11:2181,192.168.9.12:2181,192.168.9.13:2181", SESSION_TIMEOUT, event -> LOGGER.info("process : " + event.getType()));
    }

    /**
     * 关闭连接
     */
    @AfterAll
    public void close() {
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建一个znode
     * 1.CreateMode 取值
     * PERSISTENT：持久化，这个目录节点存储的数据不会丢失
     * PERSISTENT_SEQUENTIAL：顺序自动编号的目录节点，这种目录节点会根据当前已近存在的节点数自动加 1，然后返回给客户端已经成功创建的目录节点名；
     * EPHEMERAL：临时目录节点，一旦创建这个节点的客户端与服务器端口也就是 session过期超时，这种节点会被自动删除
     * EPHEMERAL_SEQUENTIAL：临时自动编号节点
     * <br>------------------------------<br>
     */
    @Test
    public void testCreate() {
        String result = null;
        try {
            result = zooKeeper.create("/zk", "zk001data".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
        LOGGER.info("create result : {}", result);
    }

    /**
     * 删除节点  忽略版本
     */
    @Test
    public void testDelete() {
        try {
            zooKeeper.delete("/zk001", -1);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
    }

    /**
     * 获取数据
     */
    @Test
    public void testGetData() {
        String result = null;
        try {
            byte[] bytes = zooKeeper.getData("/zk", null, null);
            result = new String(bytes);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
        LOGGER.info("getdata result : {}", result);
    }

    @Test
    public void testGetData01() throws Exception {
        String result = null;
        try {
            byte[] bytes = zooKeeper.getData("/zk", null, null);
            result = new String(bytes);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
        LOGGER.info("getdata result : {}", result);

        Thread.sleep(20000);

        byte[] bytes;
        try {
            bytes = zooKeeper.getData("/zk", null, null);
            result = new String(bytes);
        } catch (KeeperException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        LOGGER.info("getdata result : {}", result);
    }

    /**
     * 获取数据  设置watch
     * stat对象用来接收要查询的节点的stat
     */
    @Test
    public void testGetDataWatch() {
        String result = null;
        Stat stat = new Stat();
        try {
            System.out.println("get:");
            byte[] bytes = zooKeeper.getData("/zk", event -> {
                LOGGER.info("testGetDataWatch  watch : {}", event.getType());
                System.out.println("watcher ok");
                // 再次启动监听,这里的watch是默认的，也就是创建zk连接时的watch
                try {
                    zooKeeper.getChildren("/", true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, stat);
            System.out.println(stat.toString());
            result = new String(bytes);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
        LOGGER.info("getdata result : {}", result);

        // 触发wacth  NodeDataChanged
        try {
            System.out.println("set:");
            zooKeeper.setData("/zk", "testSetData".getBytes(), -1);
            System.out.println("set:");
            zooKeeper.setData("/zk", "testSetData".getBytes(), -1);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
        System.out.println("over");
    }

    /**
     * 判断节点是否存在
     * 设置是否监控这个目录节点，这里的 watcher 是在创建 ZooKeeper实例时指定的 watcher
     */
    @Test
    public void testExists() {
        Stat stat = null;
        try {
            stat = zooKeeper.exists("/zk001", false);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertNotNull(stat);
        LOGGER.info("exists result : {}", stat.getCzxid());
    }

    /**
     * 设置对应znode下的数据  ,  -1表示匹配所有版本
     */
    @Test
    public void testSetData() {
        Stat stat = null;
        try {
            stat = zooKeeper.setData("/zk001", "testSetData".getBytes(), -1);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertNotNull(stat);
        LOGGER.info("exists result : {}", stat.getVersion());
    }

    /**
     * 判断节点是否存在,
     * 设置是否监控这个目录节点，这里的 watcher 是在创建 ZooKeeper实例时指定的 watcher
     */
    @Test
    public void testExistsWatch1() {
        Stat stat = null;
        try {
            stat = zooKeeper.exists("/zk001", true);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertNotNull(stat);

        try {
            zooKeeper.delete("/zk001", -1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断节点是否存在,
     * 设置监控这个目录节点的 Watcher
     */
    @Test
    public void testExistsWatch2() {
        Stat stat = null;
        try {
            stat = zooKeeper.exists("/zk002", new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    LOGGER.info("testExistsWatch2  watch : {}", event.getType());
                }
            });
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
        Assert.assertNotNull(stat);

        // 触发watch 中的process方法   NodeDataChanged
        try {
            zooKeeper.setData("/zk002", "testExistsWatch2".getBytes(), -1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 不会触发watch 只会触发一次
        try {
            zooKeeper.delete("/zk002", -1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取指定节点下的子节点
     */
    @Test
    public void testGetChild() {
        try {
            zooKeeper.create("/zk/001", "001".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zooKeeper.create("/zk/002", "002".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            List<String> list = zooKeeper.getChildren("/zk", true);
            for (String node : list) {
                LOGGER.info("fffffff {}", node);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
    }
}
