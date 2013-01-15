package dk.statsbiblioteket.doms.akubra_jdbc;

import org.akubraproject.BlobStoreConnection;
import org.akubraproject.impl.AbstractBlobStore;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.AnnotationConfiguration;
import org.hibernate.cfg.Configuration;

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

    private SessionFactory sessionFactory;

    public JdbcBlobStore(URI id, File hibernateConfig) {
        super(id);
        sessionFactory = new Configuration()
                .addAnnotatedClass(HibernateBlob.class)
                .configure(hibernateConfig)
                .buildSessionFactory();


    }

    public BlobStoreConnection openConnection(Transaction transaction, Map<String, String> stringStringMap) throws UnsupportedOperationException, IOException {
        Session session = sessionFactory.openSession();
        return new JdbcBlobStoreConnection(this,session);
    }

    public void close(){

        sessionFactory.close();
    }
}