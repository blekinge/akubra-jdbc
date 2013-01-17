package dk.statsbiblioteket.doms.akubra_jdbc.util;

import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 1/17/13
 * Time: 9:24 AM
 * To change this template use File | Settings | File Templates.
 */
public interface ClosableIterator<T> extends Iterator<T> {

    public boolean close();
}
