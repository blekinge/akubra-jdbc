package dk.statsbiblioteket.doms.akubra_jdbc.util;

import dk.statsbiblioteket.doms.akubra_jdbc.util.ClosableIterator;
import org.akubraproject.BlobStoreConnection;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 1/17/13
 * Time: 9:10 AM
 * To change this template use File | Settings | File Templates.
 */
public class IteratorManager<T> {

    private Map<BlobStoreConnection,Set<ClosableIterator<T>>> map = new HashMap<BlobStoreConnection, Set<ClosableIterator<T>>>();

    public  synchronized Iterator<T> manageIterator(BlobStoreConnection connnection, ClosableIterator<T> iterator){
        Set<ClosableIterator<T>> set = map.get(connnection);
        if (set == null){
            set = new HashSet<ClosableIterator<T>>();
        }
        set.add(iterator);
        map.put(connnection,set);
        return iterator;
    }

    public synchronized void connectionClosed(BlobStoreConnection connection) {
        Set<ClosableIterator<T>> set = map.get(connection);
        if (set != null){
            for (ClosableIterator<T> closableIterator : set) {
                closableIterator.close();
            }
            map.remove(connection);
        }

    }
}
