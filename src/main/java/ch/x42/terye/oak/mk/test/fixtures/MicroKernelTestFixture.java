package ch.x42.terye.oak.mk.test.fixtures;

import org.apache.jackrabbit.mk.api.MicroKernel;

/**
 * Interface definition for test fixtures that are passed as a parameter to
 * parameterized tests.
 */
public interface MicroKernelTestFixture {

    public MicroKernel createMicroKernel() throws Exception;

    public void disposeMicroKernel(MicroKernel mk) throws Exception;

    public void setUpBeforeTest() throws Exception;

    /**
     * This method must dispose of all microkernels created by this fixture that
     * have not explicitely been disposed of.
     */
    public void tearDownAfterTest() throws Exception;

}
