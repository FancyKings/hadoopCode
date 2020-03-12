package hadoop.hbase;

import hadoop.config.HadoopConfig;
import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Test {

    static FastReader fastReader = HadoopConfig.fastReader;
    static FastWriter fastWriter = HadoopConfig.fastWriter;

    protected static Connection connection;
    private static final String ZK_QUORUM = "hbase.zookeeper.quorum";
    private static final String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
    /**
     * HBase位置
     */
    private static final String HBASE_POS = "127.0.0.1";

    /**
     * ZooKeeper位置
     */
    private static final String ZK_POS = "127.0.0.1";

    /**
     * zookeeper服务端口
     */
    private final static String ZK_PORT_VALUE = "2181";

    static{
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "127.0.0.1:9000/hbase");
        configuration.set(ZK_QUORUM, ZK_POS);
        configuration.set(ZK_CLIENT_PORT, ZK_PORT_VALUE);
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }// 创建连接池
    }


    public static void createTable() throws Exception {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
//        Admin admin = connection.getAdmin();
//        TableName tableName = TableName.valueOf("cute");
//        TableDescriptorBuilder tableDescriptor =
//                TableDescriptorBuilder.newBuilder(tableName);
//        ColumnFamilyDescriptor family =
//                ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("data")).build();
//        tableDescriptor.setColumnFamily(family);
//        admin.createTable(tableDescriptor.build());
    }

    public static void insertInfo() throws Exception {
        fastWriter.println("O");
        Configuration config = HBaseConfiguration.create();
        fastWriter.println("I");
        Connection connection = ConnectionFactory.createConnection(config);
        TableName tableName = TableName.valueOf("tb_step2");
        Table table = connection.getTable(tableName);
        try {
            byte[] row1 = Bytes.toBytes("row1");
            byte[] row2 = Bytes.toBytes("row2");
            Put put1 = new Put(row1);
            Put put2 = new Put(row2);
            byte[] columnFamily1 = Bytes.toBytes("data");
            byte[] columnFamily2 = Bytes.toBytes("data");
            byte[] qualifier1 = Bytes.toBytes(String.valueOf(1));
            byte[] qualifier2 = Bytes.toBytes(String.valueOf(2));
            byte[] value1 = Bytes.toBytes("张三丰");
            byte[] value2 = Bytes.toBytes("张无忌");
            put1.addColumn(columnFamily1, qualifier1, value1);
            put2.addColumn(columnFamily2, qualifier2, value2);
            table.put(put1);
            table.put(put2);
        } finally {
            table.close();
        }
    }

    public static void main(String[] args) throws Exception {
        createTable();
    }
}