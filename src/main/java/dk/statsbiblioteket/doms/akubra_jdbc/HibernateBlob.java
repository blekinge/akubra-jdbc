package dk.statsbiblioteket.doms.akubra_jdbc;

import org.hibernate.annotations.NaturalId;

import javax.persistence.*;
import java.sql.Blob;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 1/14/13
 * Time: 1:53 PM
 * To change this template use File | Settings | File Templates.
 */
@Entity
@Table(name = "BLOBS")
public class HibernateBlob{
    public HibernateBlob() {
    }

    /**
     * The pid of the Object
     */

    @NaturalId()
    @Id()
    private String id;

    @Lob()
    @Column(name = "BLOB", unique = false,updatable = true,insertable = true)
    private Blob blobValue;

    public HibernateBlob(String id,Blob blobValue) {
        this.id = id;
        this.blobValue = blobValue;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Blob getBlobValue() {

        return blobValue;
    }

    public void setBlobValue(Blob blobValue) {
        this.blobValue = blobValue;
    }

    @Override
    public String toString() {
        return "HibernateBlob{" +
                "id='" + id + '\'' +
                ", blobValue=" + blobValue +
                '}';
    }
}
