package hadoop.hbase;

import hadoop.config.HBaseConfig;
import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.TableDescriptor;

import java.io.IOException;

/**
 * @author FancyKing
 */
public class DeleteAll {

    static FastReader fastReader = HBaseConfig.fastReader;
    static FastWriter fastWriter = HBaseConfig.fastWriter;

    public static void main(String[] args) throws IOException {

        args = readArgs(args);
        deleteAllData(args[0]);
    }

    private static void deleteAllData(String db) throws IOException {

        Connection connection = HBaseConfig.connection;
        TableName tableName = TableName.valueOf(db);
        Admin admin = connection.getAdmin();
        // disable
        if (admin.isTableEnabled(tableName)) {
            admin.disableTable(tableName);
        }
        TableDescriptor tableDescriptor =
                admin.getDescriptor(tableName);
        admin.deleteTable(tableName);
        admin.createTable(tableDescriptor);
        // enable
        if (admin.isTableDisabled(tableName)) {
            admin.enableTable(tableName);
        }
        fastWriter.println("Database [" + tableName + "] is now clean.");
        connection.close();
    }

    private static String[] readArgs(String[] args) throws IOException {

        if (args.length == 0) {
            String[] ans = new String[1];
            fastWriter.println("database name");
            ans[0] = fastReader.nextLine();
            args = ans;
        }
        return args;
    }
}
