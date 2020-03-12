package hadoop.hbase;

import hadoop.config.HBaseConfig;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author FancyKing
 */
public class WriteBase {

    public static void main(String[] args) throws IOException {

        Connection connection = HBaseConfig.connection;
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("cute");
        TableDescriptorBuilder tableDescriptor =
                TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptor family =
                ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("data")).build();
        tableDescriptor.setColumnFamily(family);
        admin.createTable(tableDescriptor.build());
    }
}
