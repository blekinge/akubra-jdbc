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

    private HibernateBlob blob;

    private final Session session;

    public JdbcBlob(JdbcBlobStoreConnection owner, URI uri, HibernateBlob blob, Session session) {
        super(owner, uri);    //To change body of overridden methods use File | Settings | File Templates.
        this.blob = blob;
        this.session = session;

    }


    public InputStream openInputStream() throws IOException, MissingBlobException {
        log.info("Attempting to open inputstream for {}", getId());
        if (!exists()) {
            throw new MissingBlobException(id);
        }
        try {
            ensureLoaded();
            InputStream stream = blob.getBlobValue().getBinaryStream();
            //stream.read();
            return stream;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public OutputStream openOutputStream(long estimatedSize, boolean overwrite)
            throws IOException, DuplicateBlobException {
        log.info("Attempting to open outputstream for {}", getId());
        if (!exists()) {
            log.info("object {} does not exist, create an empty object", getId());
            blob = new HibernateBlob(getId().toString(), session.getLobHelper().createBlob(new byte[0]));
            session.saveOrUpdate(blob);
            session.flush();
            session.refresh(blob);
        }
        if (!overwrite && getSize() > 0) {
            throw new DuplicateBlobException(getId());
        }

        ensureOpen();
        return new JdbcOutputstream(blob, session);
    }

    public long getSize() throws IOException, MissingBlobException {
        log.info("getSize called {}", getId());
        if (!exists()) {
            throw new MissingBlobException(id);
        }
        try {
            ensureLoaded();
            long size = blob.getBlobValue().length();
            log.info("size of blob {} is {}", getId(), size);
            return size;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private void ensureLoaded() {
        if (blob == null || !session.contains(blob)){
            blob = (HibernateBlob) session.get(HibernateBlob.class,getId().toString());
        }
    }

    public boolean exists() throws IOException {
        boolean exists = session.createCriteria(HibernateBlob.class).add(Restrictions.naturalId().set("id", this.getId().toString())).uniqueResult()
                != null;
        return exists;
    }

    public void delete() throws IOException {
        log.info("Blob deleted {}", getId());
        if (exists()) {
            session.delete(blob);
            session.flush();
            blob = null;
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
