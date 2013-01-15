package dk.statsbiblioteket.doms.akubra_jdbc;

import com.google.common.collect.AbstractIterator;
import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 1/14/13
 * Time: 1:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class JdbcBlobStoreIterator extends AbstractIterator<URI> {
    private final ScrollableResults result;


    public JdbcBlobStoreIterator(Session session, String filterPrefix) {
        Criteria query = session.createCriteria(HibernateBlob.class).setFetchSize(1000).add(
                Restrictions.like("id", filterPrefix+"%"));

        result = query.scroll();


    }

    @Override
    protected URI computeNext() {
        if (result.next()){
            try {
                return new URI(((HibernateBlob)result.get(0)).getId());
            } catch (URISyntaxException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                return null;
            }
        } else {
            return endOfData();
        }
    }
}