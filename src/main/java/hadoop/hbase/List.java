package hadoop.hbase;

import hadoop.config.HBaseConfig;
import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * @author FancyKing
 */

public class List {

    static FastReader fastReader = HBaseConfig.fastReader;
    static FastWriter fastWriter = HBaseConfig.fastWriter;

    public static void main(String[] args) throws IOException {

        Connection connection = HBaseConfig.connection;
        Admin admin = connection.getAdmin();

        TableName[] tableNames = admin.listTableNames();
        fastWriter.println("Following are all table names of hbase:");
        for (TableName tableName : tableNames) {
            fastWriter.println(tableName);
        }
        connection.close();
    }

}
