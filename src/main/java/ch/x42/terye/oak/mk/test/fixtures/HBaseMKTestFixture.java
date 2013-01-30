package ch.x42.terye.oak.mk.test.fixtures;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.jackrabbit.mk.api.MicroKernel;

import ch.x42.terye.mk.hbase.HBaseMicroKernel;

public class HBaseMKTestFixture implements MicroKernelTestFixture {

    private static final String HBASE_ZOOKEEPER_QUORUM = "localhost";

    private Configuration config;
    // keep track of one of the created microkernels
    private HBaseMicroKernel mk;

    public HBaseMKTestFixture() {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);
    }

    @Override
    public MicroKernel createMicroKernel() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(config);
        mk = new HBaseMicroKernel(admin);
        return mk;
    }

    @Override
    public void setUpBeforeTest() throws Exception {
        // nothing to do, we assume the database is empty
    }

    @Override
    public void tearDownAfterTest() throws Exception {
        // this drops all tables, closes the HTable and HBaseAdmin instances (we
        // only need to do this once since all microkernels share the same
        // resources)
        if (mk != null) {
            mk.dispose(true);
        }
    }

    @Override
    public String toString() {
        return "HBaseMicroKernel";
    }

}
