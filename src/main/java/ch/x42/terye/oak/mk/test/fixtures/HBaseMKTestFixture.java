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
    private Set<HBaseMicroKernel> mks = new HashSet<HBaseMicroKernel>();

    public HBaseMKTestFixture() {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);
    }

    @Override
    public MicroKernel createMicroKernel() throws Exception {
        config.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM);
        HBaseAdmin admin = new HBaseAdmin(config);
        HBaseMicroKernel mk = new HBaseMicroKernel(admin);
        mks.add(mk);
        return mk;
    }

    @Override
    public void setUpBeforeTest() throws Exception {
        // nothing to do, we assume the database is empty
    }

    @Override
    public void tearDownAfterTest() throws Exception {
        // dispose of microkernels
        HBaseMicroKernel any = null;
        for (HBaseMicroKernel mk : mks) {
            if (any == null) {
                any = mk;
            } else {
                mk.dispose();
            }
        }
        // call this method only once in order to drop all tables and close all
        // HBase resources (which are shared among the instances)
        any.dispose(true, true);
    }

    @Override
    public String toString() {
        return "HBaseMicroKernel";
    }

}
