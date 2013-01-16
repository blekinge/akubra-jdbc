package dk.statsbiblioteket.doms.akubra_jdbc;

import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 1/15/13
 * Time: 10:23 AM
 * To change this template use File | Settings | File Templates.
 */
public class JdbcOutputstream extends ByteArrayOutputStream {


    Logger log = LoggerFactory.getLogger(JdbcOutputstream.class);
    private HibernateBlob blob;
    private String id;
    private Session session;

    public JdbcOutputstream(HibernateBlob blob, Session session) {
        this.blob = blob;
        this.session = session;
    }

    public JdbcOutputstream(String id, Session session) {
        this.id = id;

        this.session = session;
    }


    @Override
    public void close() throws IOException {

        log.info("Closing outputstream for {}",blob);
        super.close();    //To change body of overridden methods use File | Settings | File Templates.
        byte[] result = this.toByteArray();
        if (session.isOpen()){
            Transaction transaction = session.beginTransaction();
            Blob blobValue = session.getLobHelper().createBlob(result);
            if (blob == null){
                blob = new HibernateBlob(id,blobValue);
            } else {
                blob.setBlobValue(blobValue);
            }
            session.saveOrUpdate(blob);
            transaction.commit();
            session.evict(blob);
        }

    }
}
