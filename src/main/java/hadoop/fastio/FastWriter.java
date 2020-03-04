package hadoop.fastio;

import java.io.*;

/**
 * @author FancyKing
 */
public class FastWriter implements Closeable {
    private BufferedWriter writer;

    public FastWriter(OutputStream outputStream) {
        writer = new BufferedWriter(new OutputStreamWriter(outputStream));
    }

    public void print(Object object) throws IOException {
        writer.write(object.toString());
        writer.flush();
    }

    public void println(Object object) throws IOException {
        writer.write(object.toString());
        writer.write("\n");
        writer.flush();
    }

    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void close() throws IOException {
        writer.flush();
        writer.close();
    }
}
