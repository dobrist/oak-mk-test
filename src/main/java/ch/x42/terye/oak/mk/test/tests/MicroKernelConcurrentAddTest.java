package ch.x42.terye.oak.mk.test.tests;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.junit.After;
import org.junit.Before;

import ch.x42.terye.oak.mk.test.PerformanceTest;
import ch.x42.terye.oak.mk.test.fixtures.MicroKernelTestFixture;

/**
 * This test measures the performance of many threads concurrently adding a
 * large number of nodes without conflicting each other. The test creates
 * NB_THREADS threads, each of which commits a separate subtree of height
 * TREE_HEIGHT and branching factor TREE_BRANCHING_FACTOR. The workers commit
 * their nodes in batches of COMMIT_RATE nodes per commit call.
 */
public class MicroKernelConcurrentAddTest extends MicroKernelPerformanceTest {

    private static final int NB_THREADS = Runtime.getRuntime()
            .availableProcessors();
    private static final int TREE_HEIGHT = 5;
    private static final int TREE_BRANCHING_FACTOR = 6;
    private static final int COMMIT_RATE = 500;

    public List<Callable<String>> workers;

    public MicroKernelConcurrentAddTest(MicroKernelTestFixture ctx) {
        super(ctx);
    }

    @Before
    public void setUpTest() throws Exception {
        // log info
        int nbPerThread = (int) ((Math.pow(TREE_BRANCHING_FACTOR,
                TREE_HEIGHT + 1) - 1) / (TREE_BRANCHING_FACTOR - 1));
        int nbTotal = NB_THREADS * nbPerThread;
        logger.debug("Number of threads: " + NB_THREADS);
        logger.debug("Number of nodes per thread: " + nbPerThread);
        logger.debug("Total number of nodes: " + nbTotal);
        logger.debug("Creating workers");

        // create workers
        workers = new LinkedList<Callable<String>>();
        for (int i = 0; i < NB_THREADS; i++) {
            MicroKernel mk = createMicroKernel();
            // commit the root nodes of the trees
            String node = "node_" + i;
            mk.commit("/", "+\"" + node + "\":{}", null, "");
            // create worker
            Callable<String> worker = new TreeCommitter(mk, "/" + node,
                    TREE_HEIGHT, TREE_BRANCHING_FACTOR, COMMIT_RATE);
            workers.add(worker);
        }
    }

    @PerformanceTest(nbWarmupRuns = 3, nbRuns = 3)
    public void test() throws Exception {
        // run them
        logger.debug("Starting concurrent worker execution");
        ExecutorService executor = Executors.newFixedThreadPool(NB_THREADS);
        List<Future<String>> futures = new LinkedList<Future<String>>();
        for (Callable<String> worker : workers) {
            futures.add(executor.submit(worker));
        }
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
        // get return value of workers (this forces exceptions that might have
        // happened during execution to be re-thrown)
        for (Future<String> future : futures) {
            future.get();
        }
        logger.debug("All workers are done");
    }

    @After
    public void tearDownTest() {
        workers = null;
        System.gc();
    }

}
