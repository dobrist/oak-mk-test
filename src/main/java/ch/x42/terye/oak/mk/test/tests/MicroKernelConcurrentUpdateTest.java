package ch.x42.terye.oak.mk.test.tests;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.junit.After;
import org.junit.Before;

import ch.x42.terye.oak.mk.test.PerformanceTest;
import ch.x42.terye.oak.mk.test.fixtures.MicroKernelTestFixture;

/**
 * This test measures the performance of many threads concurrently updating an
 * existing tree.
 */
public class MicroKernelConcurrentUpdateTest extends MicroKernelPerformanceTest {

    private static final int NB_THREADS = Runtime.getRuntime()
            .availableProcessors();
    private static final int TREE_HEIGHT = 5;
    private static final int TREE_BRANCHING_FACTOR = NB_THREADS;
    // number of updates to be performed by each thread
    private static final int NB_UPDATES = 5000;
    // percentage of updates to the preferred subtree
    private static final double PCT_LOCAL_UPDATES = 0.8;
    // percentage of adding nodes as opposed to setting properties
    private static final double PCT_ADD_NODES = 0.5;
    private static final int COMMIT_RATE = 200;

    public List<Callable<String>> workers;
    private AtomicInteger counter;

    public MicroKernelConcurrentUpdateTest(MicroKernelTestFixture ctx) {
        super(ctx);
    }

    @Before
    public void setUpTest() throws Exception {
        // log info
        logger.debug("Number of threads: " + NB_THREADS);
        logger.debug("Creating initial tree");

        // commit initial tree
        MicroKernel mk = createMicroKernel();
        TreeCommitter committer = new TreeCommitter(mk, "/", TREE_HEIGHT,
                NB_THREADS, 1000);
        committer.call();

        // create workers
        logger.debug("Creating workers");
        counter = new AtomicInteger(NB_THREADS);
        workers = new LinkedList<Callable<String>>();
        for (int i = 0; i < NB_THREADS; i++) {
            // create worker
            mk = createMicroKernel();
            Callable<String> worker = new RandomTreeUpdater(mk, i);
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

    private class RandomTreeUpdater implements Callable<String> {

        private MicroKernel mk;
        private int preferred;
        private Random random;

        /**
         * Constructor.
         * 
         * @param mk the microkernel used to commit
         * @param preferred the index of the root's child to be used as the root
         *            of the preferred subtree
         */
        public RandomTreeUpdater(MicroKernel mk, int preferred) {
            this.mk = mk;
            this.preferred = preferred;
            this.random = new Random(hashCode());
        }

        @Override
        public String call() throws Exception {
            String revisionId = null;
            String batch = "";
            int batchCount = 0;
            for (int i = 0; i <= NB_UPDATES; i++) {
                // generate statement
                // updated the preferred subtree
                int index = preferred;
                if (random.nextDouble() >= PCT_LOCAL_UPDATES) {
                    // update any other subtree
                    while (index == preferred) {
                        index = random.nextInt(NB_THREADS);
                    }
                }
                // add new statement to batch for later commit
                batch += generateStatement(index);
                batchCount++;
                // commit batch
                if (batchCount == COMMIT_RATE) {
                    revisionId = mk.commit("/", batch, null, "");
                    batch = "";
                    batchCount = 0;
                }
            }
            // commit remaining statements, if any
            if (batchCount > 0) {
                // commit batch
                revisionId = mk.commit("/", batch, null, "");
            }
            // return last revision id
            return revisionId;
        }

        /**
         * This generates a "add node" or "set property" statement at a
         * (uniformly distributed) random node in the subtree with the specified
         * index.
         */
        private String generateStatement(int index) {
            // generate the path of a random node
            String prefix = TreeCommitter.NODE_PREFIX;
            String path = prefix + index;
            double nodeProbability = 1.0 / (int) ((Math.pow(
                    TREE_BRANCHING_FACTOR, TREE_HEIGHT + 1) - 1) / (TREE_BRANCHING_FACTOR - 1));
            for (int i = 0; i < TREE_HEIGHT - 1; i++) {
                if (random.nextDouble() < nodeProbability
                        * Math.pow(TREE_BRANCHING_FACTOR, i)) {
                    break;
                }
                path += "/" + prefix + random.nextInt(TREE_BRANCHING_FACTOR);
            }
            // generate statement
            int nb = counter.incrementAndGet();
            if (random.nextDouble() < PCT_ADD_NODES) {
                // add a new child node
                path = PathUtils.concat(path, "node_" + nb);
                return "+\"" + path + "\":{}";
            } else {
                // set a random property
                path = PathUtils.concat(path, "property_" + nb);
                return "^\"" + path + "\": \"abcd\"";
            }
        }

    }

}