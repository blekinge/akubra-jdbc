package dk.statsbiblioteket.doms.akubra_jdbc;

import dk.statsbiblioteket.doms.akubra_jdbc.util.ByteBufferBackedInputstream;
import org.akubraproject.*;
import org.akubraproject.impl.AbstractBlob;
import org.akubraproject.impl.StreamManager;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.ByteBuffer;
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

    //This one is not meant to stay in memory
    private transient HibernateBlob blob;

    private long size;
    private final Session session;
    private final StreamManager streamManager;

    public JdbcBlob(JdbcBlobStoreConnection owner, URI uri, Session session, StreamManager streamManager, Map<String, String> stringStringMap) {
        super(owner, uri);    //To change body of overridden methods use File | Settings | File Templates.
        this.session = session;
        this.streamManager = streamManager;
        size = -1;
    }


    public synchronized void reset(){
        log.debug("Clearing stored values for blob {}",getId());
        size = -1;
        session.evict(blob);
        blob = null;
    }

    public synchronized InputStream openInputStream() throws IOException, MissingBlobException {
        Transaction transaction = session.beginTransaction();
        log.info("Attempting to open inputstream for blob {}", getId());
        if (!exists()) {
            throw new MissingBlobException(id);
        }
        try {
            ByteBuffer bytes = readIntoMem();
            ByteBufferBackedInputstream stream = new ByteBufferBackedInputstream(bytes);
            return streamManager.manageInputStream(getConnection(),stream);
        } catch (SQLException e) {
            transaction.rollback();
            throw new IOException(e);
        } finally {
            if (!transaction.wasRolledBack()){
                transaction.commit();
            }
        }
    }

    private synchronized ByteBuffer readIntoMem() throws SQLException, IOException {

        ensureLoaded();
        size = blob.getBlobValue().length();

        log.info("Reading the contents ({} bytes) of blob {} into memory",size,getId());
        //TODO very large files?
        ByteBuffer buffer = ByteBuffer.allocate((int) size);
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

    public synchronized OutputStream openOutputStream(long estimatedSize, boolean overwrite)
            throws IOException, DuplicateBlobException {
        log.info("Attempting to open outputstream for {}", getId());
        if (!overwrite && exists() && getSize() > 0) {
            throw new DuplicateBlobException(getId());
        }

        return streamManager.manageOutputStream(getConnection(),new JdbcOutputstream(this, session));
    }

    public synchronized long getSize() throws IOException, MissingBlobException {
        log.debug("getSize called {}", getId());
        if (size > 0){
            return size;
        }

        if (!exists()) {
            throw new MissingBlobException(id);
        }
        Transaction transaction;
        transaction = session.beginTransaction();
        try {
            ensureLoaded();
            size = blob.getBlobValue().length();
            log.debug("size of blob {} is {}", getId(), size);
            session.evict(blob);
            return size;
        } catch (SQLException e) {
            transaction.rollback();
            throw new IOException(e);
        } finally {
            if (!transaction.wasRolledBack()){
                transaction.commit();
            }

        }
    }



    private synchronized HibernateBlob ensureLoaded() throws SQLException {
        if (blob != null){
            session.refresh(blob);
        } else {
            blob = (HibernateBlob) session.get(HibernateBlob.class,getId().toString());
        }
        if (blob != null){
            size = blob.getBlobValue().length();
        } else {
            size = -1;
        }
        return blob;
    }

    public synchronized boolean exists() throws IOException {
        try {
            ensureLoaded();
        } catch (SQLException e) {
            throw new IOException(e);

        }
        return size > 0;
    }

    public synchronized void delete() throws IOException {
        log.info("Blob deleted {}", getId());
        if (exists()) {
            Transaction transaction = session.beginTransaction();
            try {
                ensureLoaded();
                session.delete(blob);
                transaction.commit();
            } catch (SQLException e) {
                transaction.rollback();
                throw new IOException(e);
            }
            reset();
        }
    }

    public synchronized Blob moveTo(URI uri, Map<String, String> stringStringMap)
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
            try {
                ensureLoaded();
            } catch (SQLException e) {
                throw new IOException(e);
            }
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

    public synchronized HibernateBlob getHibernateBlob() {
        return blob;
    }
}
