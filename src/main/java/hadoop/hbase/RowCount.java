package hadoop.hbase;

import hadoop.config.HBaseConfig;
import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * @author FancyKing
 */
public class RowCount {

    static FastReader fastReader = HBaseConfig.fastReader;
    static FastWriter fastWriter = HBaseConfig.fastWriter;

    public static void main(String[] args) throws IOException {

        args = readArgs(args);
        rowCounter(args[0]);
    }

    private static void rowCounter(String db) throws IOException {

        Connection connection = HBaseConfig.connection;
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(db);
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        scan.setCaching(1000);
        ResultScanner scanner = table.getScanner(scan);
        int counter = 0;
        for (Result result : scanner) {
            ++counter;
        }
        fastWriter.println("Database [" + db + "] have " +
                counter + " rows");
        scanner.close();
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
