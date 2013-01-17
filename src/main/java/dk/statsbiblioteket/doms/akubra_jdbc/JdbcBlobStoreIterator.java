package dk.statsbiblioteket.doms.akubra_jdbc;

import com.google.common.collect.AbstractIterator;
import dk.statsbiblioteket.doms.akubra_jdbc.util.ClosableIterator;
import org.hibernate.Criteria;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 1/14/13
 * Time: 1:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class JdbcBlobStoreIterator<T> extends AbstractIterator<URI> implements ClosableIterator<URI> {

    private final String filterPrefix;
    Logger log = LoggerFactory.getLogger(JdbcBlobStoreIterator.class);
    private final ScrollableResults result;


    public JdbcBlobStoreIterator(Session session, String filterPrefix) {
        log.info("Opening iterator for prefix {}",filterPrefix);
        this.filterPrefix = filterPrefix;
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
            close();
            return endOfData();
        }
    }

    public boolean close(){
        log.info("Closing iterator for prefix {}",filterPrefix);
        result.close();
        return true;

    }
}
