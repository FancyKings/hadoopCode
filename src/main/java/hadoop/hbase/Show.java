package hadoop.hbase;

import hadoop.config.HBaseConfig;
import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author FancyKing
 */
public class Show {

    static FastReader fastReader = HBaseConfig.fastReader;
    static FastWriter fastWriter = HBaseConfig.fastWriter;

    public static void main(String[] args) throws IOException {

        args = readArgs(args);
        readData(args[0]);
    }

    private static void readData(String db) throws IOException {

        Connection connection = HBaseConfig.connection;

        TableName tableName = TableName.valueOf(db);
        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        scan.setCaching(1000);
        ResultScanner scanner = table.getScanner(scan);
        for (Result rr : scanner) {
            for (Cell cell : rr.listCells()) {
                String qualifier = Bytes.toString(cell.getQualifierArray(),
                        cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(),
                        cell.getValueOffset(), cell.getValueLength());
                String family = Bytes.toString(cell.getFamilyArray(),
                        cell.getFamilyOffset(), cell.getFamilyLength());
                String keyRow = Bytes.toString(cell.getRowArray(),
                        cell.getRowOffset(), cell.getRowLength());
                fastWriter.println("Row: " + keyRow + "\tCOLUMN+CELL: " + family + ":" + qualifier
                        + "\tValue: " + value);
            }
        }
        fastWriter.println("END SHOW");
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
