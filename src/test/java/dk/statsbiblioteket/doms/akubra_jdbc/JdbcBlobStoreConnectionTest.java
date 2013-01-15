package dk.statsbiblioteket.doms.akubra_jdbc;

import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 1/14/13
 * Time: 1:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class JdbcBlobStoreConnectionTest {

    BlobStore store;

    @Before
    public void setUp() throws Exception {
        store = new JdbcBlobStore(
                new URI("dsfsd"),
                new File(
                        Thread
                                .currentThread()
                                .getContextClassLoader()
                                .getResource("hibernate.cfg.xml")
                                .toURI()
                )
        );


    }

    @org.junit.Test
    public void testOpenConnection() throws Exception {

        BlobStoreConnection connection = store.openConnection(null, null);
        Blob blob = connection.getBlob(new URI("doms:test1"), null);
        String sample1 = "test of content";
        write(blob,sample1);
        connection.sync();
        long size = blob.getSize();
        connection.close();

        BlobStoreConnection connection2 = store.openConnection(null, null);
        Blob blob2 = connection2.getBlob(new URI("doms:test1"), null);
        long size2 = blob2.getSize();
        connection2.close();

        assertEquals("String changed while stored", size, size2);

        BlobStoreConnection connection3 = store.openConnection(null, null);
        Blob blob3 = connection3.getBlob(new URI("doms:test1"), null);

        assertEquals("String changed while stored",sample1,read(blob3));
        connection3.close();

    }
    @org.junit.Test
    public void testListIds() throws IOException, URISyntaxException {

        BlobStoreConnection connection = store.openConnection(null, null);
        Blob blob = connection.getBlob(null, null);
        System.out.println(blob.exists());
        blob = connection.getBlob(null, null);
        blob = connection.getBlob(null, null);
        blob = connection.getBlob(null, null);
        connection.sync();
        Iterator<URI> result = connection.listBlobIds("uuid");
        while (result.hasNext()) {
            URI next = result.next();
            System.out.println(next);
        }

        connection.close();
        connection.close();

    }




    @org.junit.Test
    public void testOpenConnection2() throws Exception {

        BlobStoreConnection connection = store.openConnection(null, null);
        Blob blob = connection.getBlob(null, null);
        URI id = blob.getId();
        String sample1 = "test of content";
        write(blob,sample1);
        connection.sync();
        long size = blob.getSize();
        connection.close();

        BlobStoreConnection connection2 = store.openConnection(null, null);
        Blob blob2 = connection2.getBlob(id, null);
        long size2 = blob2.getSize();
        connection2.close();

        assertEquals("String changed while stored", size, size2);

        BlobStoreConnection connection3 = store.openConnection(null, null);
        Blob blob3 = connection3.getBlob(id, null);
        assertEquals("String changed while stored",sample1,read(blob3));

        connection3.close();




    }

    @Test
    public void testDelete() throws IOException {
        BlobStoreConnection connection = store.openConnection(null, null);
        Blob blob = connection.getBlob(null, null);
        System.out.println(blob.exists());
        blob = connection.getBlob(null, null);
        assertTrue("Blob now exists", blob.exists());
        write(blob, "test2\nsgd");
        blob.delete();
        assertFalse(blob.exists());
        connection.close();

        BlobStoreConnection connection2 = store.openConnection(null, null);
        Blob newblob = connection2.getBlob(blob.getId(), null);
        assertTrue(newblob.exists());
        System.out.println(read(newblob));
    }


    private void write(Blob blob, String output) throws IOException {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(blob.openOutputStream(output.length(), true)));
        writer.write(output);
        writer.close();
    }

    private String read(Blob blob) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(blob.openInputStream()));
        String line = reader.readLine();
        if (line != null){
            StringBuilder builder = new StringBuilder(line);

            while ((line = reader.readLine()) != null) {
                builder.append("\n"+line);
            }
            return builder.toString();
        }
        return line;
    }

}