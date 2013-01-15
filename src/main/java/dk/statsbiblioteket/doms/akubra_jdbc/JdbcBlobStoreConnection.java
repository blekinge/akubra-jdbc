package dk.statsbiblioteket.doms.akubra_jdbc;

import org.akubraproject.Blob;
import org.akubraproject.UnsupportedIdException;
import org.akubraproject.impl.AbstractBlobStoreConnection;
import org.hibernate.Session;
import org.hibernate.Transaction;

import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 1/14/13
 * Time: 1:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class JdbcBlobStoreConnection extends AbstractBlobStoreConnection{

    private Session session;
    private final Transaction transaction;

    private Set<JdbcBlob> blobSet = new HashSet<JdbcBlob>();

    public JdbcBlobStoreConnection(JdbcBlobStore owner, Session session) {
        super(owner);
        this.session = session;
        transaction = session.beginTransaction();
    }

    public Blob getBlob(URI uri, Map<String, String> stringStringMap) throws IOException, UnsupportedIdException, UnsupportedOperationException {

        if (uri == null){
            uri = URI.create("uuid:"+UUID.randomUUID().toString());
        }
        Object temp = session.get(HibernateBlob.class, uri.toString());


        HibernateBlob blob = null;
        if (temp instanceof HibernateBlob) {
            blob = (HibernateBlob) temp;
        }

        if (blob == null){
            blob = new HibernateBlob(uri.toString(),session.getLobHelper().createBlob(new byte[1]));
            session.saveOrUpdate(blob);
            session.flush();
            session.refresh(blob);
        }
        JdbcBlob jdbcblob = new JdbcBlob(this, uri, blob, session);
        blobSet.add(jdbcblob);
        return  jdbcblob;
    }

    public Iterator<URI> listBlobIds(String filterPrefix) throws IOException {
        return new JdbcBlobStoreIterator(session, filterPrefix);

    }

    public void sync() throws IOException, UnsupportedOperationException {
        session.flush();
    }

    @Override
    public void close() {
        if (isClosed()){
            return;
        }
        try {
            sync();
        } catch (IOException e) {
            transaction.rollback();
        }
        transaction.commit();
        super.close();    //To change body of overridden methods use File | Settings | File Templates.

    }




}
