package dk.statsbiblioteket.doms.akubra_jdbc;

import org.akubraproject.*;
import org.akubraproject.impl.AbstractBlob;
import org.hibernate.LobHelper;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 1/14/13
 * Time: 1:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class JdbcBlob extends AbstractBlob {
    Logger log = LoggerFactory.getLogger(JdbcBlob.class);

    private transient HibernateBlob blob;

    private final Session session;

    public JdbcBlob(JdbcBlobStoreConnection owner, URI uri, Session session) {
        super(owner, uri);    //To change body of overridden methods use File | Settings | File Templates.
        this.session = session;
    }


    public InputStream openInputStream() throws IOException, MissingBlobException {
        Transaction transaction = session.beginTransaction();
        log.info("Attempting to open inputstream for {}", getId());
        if (!exists()) {
            throw new MissingBlobException(id);
        }
        try {
            ByteBuffer bytes = readIntoMem();

            return new ByteBufferBackedInputstream(bytes);
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            transaction.commit();
        }
    }

    private ByteBuffer readIntoMem() throws SQLException, IOException {

        ensureLoaded();
        //TODO very large files?
        ByteBuffer buffer = ByteBuffer.allocate((int) blob.getBlobValue().length());
        InputStream inputstream = blob.getBlobValue().getBinaryStream();
        while (true) {
            byte[] temp = new byte[1024];
            int length = inputstream.read(temp);
            if (length > 0) {
                buffer.put(temp,0,length);
            } else {
                inputstream.close();
                break;
            }
        }
        blob.getBlobValue().free();
        session.evict(blob);
        buffer.rewind();
        return buffer;
    }

    public OutputStream openOutputStream(long estimatedSize, boolean overwrite)
            throws IOException, DuplicateBlobException {
        log.info("Attempting to open outputstream for {}", getId());
        if (!overwrite && exists() && getSize() > 0) {
            throw new DuplicateBlobException(getId());
        }

        //ensureLoaded();
        if (blob == null){
            return new JdbcOutputstream(getId().toString(), session);
        }else {
            return new JdbcOutputstream(blob, session);
        }
    }

    public long getSize() throws IOException, MissingBlobException {
        log.info("getSize called {}", getId());
        if (!exists()) {
            throw new MissingBlobException(id);
        }
        Transaction transaction = null;
        try {
        transaction = session.beginTransaction();
            ensureLoaded();
            long size = blob.getBlobValue().length();
            log.info("size of blob {} is {}", getId(), size);
            session.evict(blob);
            return size;
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            transaction.commit();
        }
    }

    private void ensureLoaded() {
        session.evict(blob);
        blob = (HibernateBlob) session.get(HibernateBlob.class,getId().toString());
    }

    public boolean exists() throws IOException {
        boolean exists = session.createCriteria(HibernateBlob.class).add(Restrictions.naturalId().set("id", this.getId().toString())).uniqueResult()
                != null;
        return exists;
    }

    public void delete() throws IOException {
        log.info("Blob deleted {}", getId());
        if (exists()) {
            Transaction transaction = session.beginTransaction();
            ensureLoaded();
            session.delete(blob);

            transaction.commit();
        }
    }

    public Blob moveTo(URI uri, Map<String, String> stringStringMap)
            throws DuplicateBlobException,
            IOException,
            MissingBlobException,
            NullPointerException,
            IllegalArgumentException {
        log.info("moveTo from {} to {}", getId(), uri);
        Blob newBlob = owner.getBlob(uri, stringStringMap);
        if (!exists()) {
            throw new MissingBlobException(getId());
        } else {
            ensureLoaded();
        }
        if (newBlob.exists()) {
            throw new DuplicateBlobException(getId());
        }
        OutputStream outputstream = newBlob.openOutputStream(getSize(), false);
        InputStream inputstream = openInputStream();
        while (true) {
            byte[] temp = new byte[1024];
            int length = inputstream.read(temp);
            if (length > 0) {
                outputstream.write(temp, 0, length);
            } else {
                outputstream.flush();
                outputstream.close();
                inputstream.close();
                break;
            }
        }
        this.delete();
        log.info("moveTo from {} to {} is now done", getId(),uri);

        return newBlob;
    }
}
