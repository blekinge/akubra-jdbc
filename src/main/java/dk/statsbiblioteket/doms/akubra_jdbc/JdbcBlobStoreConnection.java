package dk.statsbiblioteket.doms.akubra_jdbc;

import dk.statsbiblioteket.doms.akubra_jdbc.util.ClosableIterator;
import dk.statsbiblioteket.doms.akubra_jdbc.util.IteratorManager;
import org.akubraproject.Blob;
import org.akubraproject.UnsupportedIdException;
import org.akubraproject.impl.AbstractBlobStoreConnection;
import org.akubraproject.impl.StreamManager;
import org.hibernate.Session;
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

    private final StreamManager streamManager;

    private final IteratorManager iteratorManager;
    Logger log = LoggerFactory.getLogger(JdbcBlobStoreConnection.class);
    private Session session;




    public JdbcBlobStoreConnection(JdbcBlobStore owner, Session session, StreamManager streamManager,
                                   IteratorManager<ClosableIterator<URI>> iteratorManager) {
        super(owner);
        this.streamManager = streamManager;
        this.iteratorManager = iteratorManager;
        log.info("Connection opened",session);
        this.session = session;
    }

    public Blob getBlob(URI uri, Map<String, String> stringStringMap) throws IOException, UnsupportedIdException, UnsupportedOperationException {


        log.info("Attempting to retrieve blob {}",uri,stringStringMap);
        if (uri == null){
            uri = URI.create("uuid:"+UUID.randomUUID().toString());
            log.debug("uri null, created new uri {}",uri);
        }

        JdbcBlob jdbcblob = new JdbcBlob(this, uri, session,streamManager);
        return  jdbcblob;
    }

    public Iterator<URI> listBlobIds(String filterPrefix) throws IOException {
        log.info("Attempting to list all block ids with prefix {}",filterPrefix,session);
        return iteratorManager.manageIterator(this, new JdbcBlobStoreIterator(session, filterPrefix));

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
        session.flush();
        streamManager.connectionClosed(this);
        iteratorManager.connectionClosed(this);
        super.close();    //To change body of overridden methods use File | Settings | File Templates.

    }

}
