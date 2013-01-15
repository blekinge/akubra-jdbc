package dk.statsbiblioteket.doms.akubra_jdbc;

import org.akubraproject.*;
import org.akubraproject.impl.AbstractBlob;
import org.hibernate.LobHelper;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;

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


    private HibernateBlob blob;

    private final Session session;

    public JdbcBlob(JdbcBlobStoreConnection owner, URI uri, HibernateBlob blob, Session session) {
        super(owner, uri);    //To change body of overridden methods use File | Settings | File Templates.
        this.blob = blob;
        this.session = session;
    }

    public InputStream openInputStream() throws IOException, MissingBlobException {
        if (!exists()) {
            throw new MissingBlobException(id);
        }
        try {
            InputStream stream = blob.getBlobValue().getBinaryStream();
            //stream.read();
            return stream;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public OutputStream openOutputStream(long estimatedSize, boolean overwrite)
            throws IOException, DuplicateBlobException {
        if (!overwrite && getSize() > 0) {
            throw new DuplicateBlobException(getId());
        }
        return new JdbcOutputstream(blob, session);
    }

    public long getSize() throws IOException, MissingBlobException {
        if (!exists()) {
            throw new MissingBlobException(id);
        }
        try {
            return blob.getBlobValue().length();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public boolean exists() throws IOException {
        return
                session.createCriteria(HibernateBlob.class).add(Restrictions.naturalId().set("id", this.getId().toString())).uniqueResult()
                        != null;
    }

    public void delete() throws IOException {
        session.delete(blob);
    }

    public Blob moveTo(URI uri, Map<String, String> stringStringMap)
            throws DuplicateBlobException,
            IOException,
            MissingBlobException,
            NullPointerException,
            IllegalArgumentException {
        Blob newBlob = owner.getBlob(uri, stringStringMap);
        OutputStream outputstream = newBlob.openOutputStream(getSize(), false);
        InputStream inputstream = openInputStream();
        while (true) {
            byte[] temp = new byte[1024];
            int length = inputstream.read(temp);
            if (length > 0) {
                outputstream.write(temp, 0, length);
            } else {
                outputstream.close();
                inputstream.close();
                break;
            }
        }
        this.delete();
        return newBlob;
    }
}
