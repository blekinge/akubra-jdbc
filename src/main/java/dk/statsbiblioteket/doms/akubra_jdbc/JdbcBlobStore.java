package dk.statsbiblioteket.doms.akubra_jdbc;

import dk.statsbiblioteket.doms.akubra_jdbc.util.ClosableIterator;
import dk.statsbiblioteket.doms.akubra_jdbc.util.IteratorManager;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.impl.AbstractBlobStore;
import org.akubraproject.impl.StreamManager;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.Transaction;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 1/14/13
 * Time: 1:02 PM
 * To change this template use File | Settings | File Templates.
 */
public class JdbcBlobStore extends AbstractBlobStore {

    Logger log = LoggerFactory.getLogger(JdbcBlobStore.class);

    private SessionFactory sessionFactory;
    private StreamManager streamManager;
    private IteratorManager<ClosableIterator<URI>> iteratorManager;



    public JdbcBlobStore(URI id, File hibernateConfig) {
        super(id);
        sessionFactory = new Configuration()
                .addAnnotatedClass(HibernateBlob.class)
                .configure(hibernateConfig)
                .buildSessionFactory();
        streamManager = new StreamManager();
        iteratorManager = new IteratorManager<ClosableIterator<URI>>();

    }

    public BlobStoreConnection openConnection(Transaction transaction, Map<String, String> stringStringMap) throws UnsupportedOperationException, IOException {
        log.info("attempting to open connection to store {}",getId());
        Session session = sessionFactory.openSession();
        return new JdbcBlobStoreConnection(this,session, streamManager,iteratorManager);
    }


}
