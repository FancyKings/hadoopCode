package hadoop.config;

import hadoop.fastio.FastReader;
import hadoop.fastio.FastWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author FancyKing
 */
public class HBaseConfig {

    public static Configuration config = HBaseConfiguration.create();
    public static Connection connection;
    public static FastReader fastReader = new FastReader(System.in);
    public static FastWriter fastWriter = new FastWriter(System.out);

    static {
        try {
            connection = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
