package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;

/**
 * Hello world!
 *
 */
public class PagingSelect
{
    public static void main( String[] args )
    {
        DriverConfigLoader loader =
                DriverConfigLoader.programmaticBuilder()
                        .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 5)
                        .build();

        try (CqlSession session = CqlSession.builder().withConfigLoader(loader).build()) {

            System.out.println( "First Result" );
            ResultSet firstResult = session.execute("SELECT * FROM uline_test.paging WHERE pkey = 'pkey1';");
            ByteBuffer firstPgSt = firstResult.getExecutionInfo().getPagingState();
            while (firstResult.getAvailableWithoutFetching() > 0) {
                Row row = firstResult.one();
                assert row != null;
                System.out.println(row.getFormattedContents());
            }

            System.out.println("Second Result");
            SimpleStatement secondStatement = SimpleStatement.builder("SELECT * FROM uline_test.paging WHERE pkey = 'pkey1';")
                    .setPagingState(firstPgSt)
                    .build();
            ResultSet secondResult = session.execute(secondStatement);
            ByteBuffer secondPgSt = secondResult.getExecutionInfo().getPagingState();
            while (secondResult.getAvailableWithoutFetching() > 0) {
                Row row = secondResult.one();
                assert row != null;
                System.out.println(row.getFormattedContents());
            }

            System.out.println("Third Result");
            SimpleStatement thirdStatement = SimpleStatement.builder("SELECT * FROM uline_test.paging WHERE pkey = 'pkey1';")
                    .setPagingState(secondPgSt)
                    .build();
            ResultSet thirdResult = session.execute(thirdStatement);
            ByteBuffer thirdPgSt = thirdResult.getExecutionInfo().getPagingState();
            while (thirdResult.getAvailableWithoutFetching() > 0) {
                Row row = thirdResult.one();
                assert row != null;
                System.out.println(row.getFormattedContents());
            }

            System.out.println("Fourth Result");
            SimpleStatement fourthStatement = SimpleStatement.builder("SELECT * FROM uline_test.paging WHERE pkey = 'pkey1';")
                    .setPagingState(thirdPgSt)
                    .build();
            ResultSet fourthResult = session.execute(fourthStatement);
            while (fourthResult.getAvailableWithoutFetching() > 0) {
                Row row = fourthResult.one();
                assert row != null;
                System.out.println(row.getFormattedContents());
            }

            System.out.println("Third Result Again");
            SimpleStatement fifthStatement = SimpleStatement.builder("SELECT * FROM uline_test.paging WHERE pkey = 'pkey1';")
                    .setPagingState(secondPgSt)
                    .build();
            ResultSet fifthResult = session.execute(fifthStatement);
            while (fifthResult.getAvailableWithoutFetching() > 0) {
                Row row = fifthResult.one();
                assert row != null;
                System.out.println(row.getFormattedContents());
            }

            session.close();
        }
    }
}
