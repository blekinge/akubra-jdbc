package dk.statsbiblioteket.doms.akubra_jdbc;

import org.akubraproject.Blob;
import org.akubraproject.UnsupportedIdException;
import org.akubraproject.impl.AbstractBlobStoreConnection;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    Logger log = LoggerFactory.getLogger(JdbcBlobStoreConnection.class);
    private Session session;
    private Transaction transaction;

    public JdbcBlobStoreConnection(JdbcBlobStore owner, Session session) {
        super(owner);
        log.info("Connection opened",session);
        this.session = session;
        log.info("Beginning transaction",session);
        transaction = session.beginTransaction();
    }

    public Blob getBlob(URI uri, Map<String, String> stringStringMap) throws IOException, UnsupportedIdException, UnsupportedOperationException {

        log.info("Attempting to retrieve blob {}",uri,stringStringMap);
        if (uri == null){
            uri = URI.create("uuid:"+UUID.randomUUID().toString());
            log.debug("uri null, created new uri {}",uri);
        }

        Object temp = session.get(HibernateBlob.class, uri.toString());
        log.info("object retrieved from storage {}",temp);

        if (temp == null){
            log.info("Object is null, defer creation {}",uri);
        }
        HibernateBlob blob = null;
        if (temp instanceof HibernateBlob) {
            blob = (HibernateBlob) temp;
        }

        JdbcBlob jdbcblob = new JdbcBlob(this, uri, blob, session);
        return  jdbcblob;
    }

    public Iterator<URI> listBlobIds(String filterPrefix) throws IOException {
        log.info("Attempting to list all block ids with prefix {}",filterPrefix,session);
        return new JdbcBlobStoreIterator(session, filterPrefix);

    }

    public void sync() throws IOException, UnsupportedOperationException {
        log.info("Sync called");
        session.flush();
    }

    @Override
    public void close() {
        if (isClosed()){
            return;
        }
        log.info("Connection closing");
        try {
            sync();
        } catch (IOException e) {
            transaction.rollback();
        }
        transaction.commit();
        super.close();    //To change body of overridden methods use File | Settings | File Templates.

    }




}
