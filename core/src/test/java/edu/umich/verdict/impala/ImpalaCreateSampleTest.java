package edu.umich.verdict.impala;

import java.io.FileNotFoundException;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import edu.umich.verdict.BaseIT;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

import com.cloudera.impala.jdbc4.Driver;

public class ImpalaCreateSampleTest extends BaseIT {

    @BeforeClass
    public static void connect() throws VerdictException, SQLException, FileNotFoundException, ClassNotFoundException {
        VerdictConf conf = new VerdictConf();
        conf.setHost(readHost());
        conf.setDbms("impala");
        conf.setPort("21050");
        conf.setDbmsSchema("tpch1g");
        conf.set("no_user_password", "true");

        vc = VerdictJDBCContext.from(conf);
    }
    
    @Test
    @Category(edu.umich.verdict.MinimumTest.class)
    public void createSamples() throws VerdictException {
        vc.executeJdbcQuery("create sample of lineitem");
    }

    @AfterClass
    public static void destroy() throws VerdictException {
        vc.destroy();
    }
}
