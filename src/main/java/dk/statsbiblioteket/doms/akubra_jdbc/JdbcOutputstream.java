package dk.statsbiblioteket.doms.akubra_jdbc;

import org.hibernate.Query;
import org.hibernate.Session;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

    private HibernateBlob blob;
    private Session session;

    public JdbcOutputstream(HibernateBlob blob, Session session) {
        this.blob = blob;
        this.session = session;
    }

    @Override
    public void close() throws IOException {
        super.close();    //To change body of overridden methods use File | Settings | File Templates.
        byte[] result = this.toByteArray();
        if (session.isOpen()){
            Query ps = session.createSQLQuery("UPDATE BLOBS SET Blob=? where Id=?");
            ps.setString(1, blob.getId());
            ps.setBinary(0, result);
            ps.executeUpdate();
            session.refresh(blob);
        }

    }
}
