package ch.x42.terye.oak.mk.test.fixtures;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.jackrabbit.mk.api.MicroKernel;

import ch.x42.terye.mk.hbase.HBaseMicroKernel;

public class HBaseMKTestFixture implements MicroKernelTestFixture {

    private static final String HBASE_ZOOKEEPER_QUORUM = "localhost";

    private Configuration config;
    // keep track of the created microkernels
    private Set<HBaseMicroKernel> mks;
    // counter for explicit machine ids
    private int counter;

    public HBaseMKTestFixture() {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);
        mks = new HashSet<HBaseMicroKernel>();
        counter = 0;
    }

    @Override
    public MicroKernel createMicroKernel() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(config);
        HBaseMicroKernel mk = new HBaseMicroKernel(admin, counter++);
        mks.add(mk);
        return mk;
    }

    @Override
    public void disposeMicroKernel(MicroKernel mk) throws Exception {
        if (mks.remove(mk)) {
            ((HBaseMicroKernel) mk).dispose();
        }
    }

    @Override
    public void setUpBeforeTest() throws Exception {
        // nothing to do, we assume the database is empty
    }

    @Override
    public void tearDownAfterTest() throws Exception {
        // dispose of remaining microkernels
        HBaseMicroKernel any = null;
        for (HBaseMicroKernel mk : mks) {
            if (any == null) {
                any = mk;
            } else {
                mk.dispose();
            }
        }
        // drop all tables using any of the microkernels
        any.dispose(true);
        // clear list so that the microkernels can be gc'd
        mks.clear();
    }

    @Override
    public String toString() {
        return "HBaseMicroKernel";
    }

}
