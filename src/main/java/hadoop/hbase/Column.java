package hadoop.hbase;

import hadoop.config.HBaseConfig;
import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author FancyKing
 */
public class Column {

    static FastReader fastReader = HBaseConfig.fastReader;
    static FastWriter fastWriter = HBaseConfig.fastWriter;

    public static void main(String[] args) throws IOException {

        args = readArgs(args);
        if ("A".equals(args[0]) || "a".equals(args[0])) {
            addFamilyAndQualifier(args[1], args[2]);
        } else if("D".equals(args[0]) || "d".equals(args[0])) {
            deleteFamilyAndQualifier(args[1], args[2]);
        } else {
            fastWriter.println("Wrong choice.");
        }
    }

    private static void addFamilyAndQualifier(String db, String cols) throws IOException {

        Connection connection = HBaseConfig.connection;
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(db);
        String[] column = cols.split(":");
        // disable
        admin.disableTable(tableName);
        ColumnFamilyDescriptor columnFamilyDescriptor =
                ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(column[0])).build();
        admin.addColumnFamily(tableName, columnFamilyDescriptor);
        // enable
        admin.enableTable(tableName);
        if (column.length == 1) {
            fastWriter.println("Family [" + column[0] + "] add to " + db);
        } else {
            fastWriter.println("Family [" + column[0] + "] qualifier [" + column[1] +
                    "] add to db");
        }
        connection.close();
    }

    private static void deleteFamilyAndQualifier(String db, String cols) throws IOException {

        Connection connection = HBaseConfig.connection;
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(db);
        String[] column = cols.split(":");
        // disable
        if (admin.isTableEnabled(tableName)) {
            admin.disableTable(tableName);
        }
        admin.deleteColumnFamily(tableName, Bytes.toBytes(column[0]));
        // enable
        if (admin.isTableDisabled(tableName)) {
            admin.enableTable(tableName);
        }
        if (column.length == 1) {
            fastWriter.println("Delete [" + column[0] + "] in " + db);
        } else {
            fastWriter.println("Delete [" + column[0] + "] qualifier [" + column[1] +
                    "] in " + db);
        }
        connection.close();
    }

    private static String[] readArgs(String[] args) throws IOException {

        if (args.length < 3) {
            String[] ans = new String[3];
            fastWriter.println("Add or Delete[A/D]:");
            ans[0] = fastReader.nextLine();
            fastWriter.println("database name");
            ans[1] = fastReader.nextLine();
            fastWriter.println("family[:qualifier]");
            ans[2] = fastReader.nextLine();
            args = ans;
        }
        return args;
    }
}
