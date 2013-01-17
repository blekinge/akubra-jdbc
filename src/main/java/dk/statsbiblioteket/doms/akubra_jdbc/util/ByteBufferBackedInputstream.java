package dk.statsbiblioteket.doms.akubra_jdbc.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 1/16/13
 * Time: 2:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class ByteBufferBackedInputstream  extends InputStream {

    ByteBuffer buf;
    public ByteBufferBackedInputstream(ByteBuffer buf){
        this.buf = buf;

    }
    public synchronized int read() throws IOException {
        if (!buf.hasRemaining()) {
            return -1;
        }
        return buf.get();
    }
    public synchronized int read(byte[] bytes, int off, int len) throws IOException {
        len = Math.min(len, buf.remaining());
        if (len == 0){
            return -1;
        }
        buf.get(bytes, off, len);
        return len;
    }


}