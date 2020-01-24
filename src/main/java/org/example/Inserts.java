package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

/**
CREATE TABLE IF NOT EXISTS uline_test.paging (
 pkey text,
 clustering1 int,
 clustering2 int,
 PRIMARY KEY((pkey),clustering1,clustering2)
);
 */
public class Inserts
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        try (CqlSession session = CqlSession.builder().build()) {
            PreparedStatement insert = session.prepare("INSERT INTO uline_test.paging (pkey, clustering1, clustering2) VALUES (?,?,?)");
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < 5000; j++) {
                    String pkey = "pkey"+i;
                    BoundStatement bInsert = insert.bind(pkey,i,j);
                    session.execute(bInsert);
                }
            }
            session.close();
        }
    }
}
