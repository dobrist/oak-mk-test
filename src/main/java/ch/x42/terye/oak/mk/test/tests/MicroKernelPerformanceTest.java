package ch.x42.terye.oak.mk.test.tests;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.x42.terye.oak.mk.test.ParameterizedPerformanceTestRunner;
import ch.x42.terye.oak.mk.test.fixtures.HBaseMKTestFixture;
import ch.x42.terye.oak.mk.test.fixtures.MicroKernelTestFixture;

/**
 * This is the base class for all MicroKernel performance tests. When run, the
 * test is executed once for each parameter array returned by the
 * getParameters() method.
 */
@RunWith(ParameterizedPerformanceTestRunner.class)
public abstract class MicroKernelPerformanceTest {

    @Parameters
    public static Collection<Object[]> getParameters() {
        List<Object[]> parameters = new LinkedList<Object[]>();
        // execute tests with the HBase microkernel
        parameters.add(new Object[] {
            new HBaseMKTestFixture()
        });
        return parameters;
    }

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected MicroKernelTestFixture fixture;

    protected MicroKernelPerformanceTest(MicroKernelTestFixture fixture) {
        this.fixture = fixture;
    }

    @Before
    public final void setUp() throws Exception {
        System.gc();
        logger.debug("Calling test setup fixture method");
        fixture.setUpBeforeTest();
    }

    @After
    public final void tearDown() throws Exception {
        logger.debug("Calling test teardown fixture method");
        fixture.tearDownAfterTest();
    }

}
