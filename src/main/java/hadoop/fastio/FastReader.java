package hadoop.fastio;

import java.io.*;
import java.math.BigInteger;
import java.util.StringTokenizer;

/**
 * @author FancyKing
 */
public class FastReader implements Closeable {
    private BufferedReader reader;
    private StringTokenizer tokenizer;

    public FastReader(InputStream inputStream) {
        reader = new BufferedReader(new InputStreamReader(inputStream));
        tokenizer = new StringTokenizer("");
    }

    private String innerNextLine() {
        try {
            return reader.readLine();
        } catch (IOException ex) {
            return null;
        }
    }

    public void hasNext() {
        while (!tokenizer.hasMoreTokens()) {
            String nextLine = innerNextLine();
            if (nextLine == null) {
                return;
            }
            tokenizer = new StringTokenizer(nextLine);
        }
    }

    public String nextLine() {
        tokenizer = new StringTokenizer("");
        return innerNextLine();
    }

    public String next() {
        hasNext();
        return tokenizer.nextToken();
    }

    public int nextInt() {
        return Integer.parseInt(next());
    }

    public double nextDouble() {
        return Double.parseDouble(next());
    }

    public BigInteger nextBigInteger() {
        return new BigInteger(next());
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
