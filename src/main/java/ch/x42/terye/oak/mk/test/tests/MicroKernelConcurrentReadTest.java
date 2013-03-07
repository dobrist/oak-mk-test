package ch.x42.terye.oak.mk.test.tests;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.junit.Before;

import ch.x42.terye.oak.mk.test.PerformanceTest;
import ch.x42.terye.oak.mk.test.fixtures.MicroKernelTestFixture;

/**
 * This test measures the performance of many threads concurrently reading an
 * existing tree.
 */
public class MicroKernelConcurrentReadTest extends MicroKernelPerformanceTest {

    private static final int NB_THREADS = Runtime.getRuntime()
            .availableProcessors();
    private static final int TREE_HEIGHT = 5;
    private static final int TREE_BRANCHING_FACTOR = 6;
    // number of read operations to be performed by each thread
    private static final int NB_READS = 5000;
    // percentage of read operations from the preferred subtree
    private static final double PCT_LOCAL_READS = 0.8;

    public List<Callable<String>> workers;

    public MicroKernelConcurrentReadTest(MicroKernelTestFixture fixture) {
        super(fixture);
    }

    @Before
    public void setUpTest() throws Exception {
        // log info
        logger.debug("Number of threads: " + NB_THREADS);
        logger.debug("Creating initial tree");

        // commit initial tree
        MicroKernel mk = fixture.createMicroKernel();
        for (int i = 0; i < NB_THREADS; i++) {
            // commit the root nodes of the subtrees
            String node = "node_" + i;
            mk.commit("/", "+\"" + node + "\":{}", null, "");
            TreeCommitter committer = new TreeCommitter(mk, "/" + node,
                    TREE_HEIGHT, TREE_BRANCHING_FACTOR, 1000);
            committer.call();
        }
        fixture.disposeMicroKernel(mk);

        // create workers
        logger.debug("Creating workers");
        workers = new LinkedList<Callable<String>>();
        for (int i = 0; i < NB_THREADS; i++) {
            mk = fixture.createMicroKernel();
            Callable<String> worker = new Reader(mk, i);
            workers.add(worker);
        }
    }

    @PerformanceTest(nbWarmupRuns = 2, nbRuns = 3)
    public void test() throws Exception {
        // run them
        logger.debug("Starting concurrent worker execution");
        ExecutorService executor = Executors.newFixedThreadPool(NB_THREADS);
        List<Future<String>> futures = new LinkedList<Future<String>>();
        for (Callable<String> worker : workers) {
            futures.add(executor.submit(worker));
        }
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.MINUTES);
        // get return value of workers (this forces exceptions that might have
        // happened during execution to be re-thrown)
        for (Future<String> future : futures) {
            future.get();
        }
        logger.debug("All workers are done");
    }

    private class Reader implements Callable<String> {

        private MicroKernel mk;
        private int preferred;
        private Random random;

        /**
         * @param mk the microkernel used to commit
         * @param preferred the index of the root's child to be used as the the
         *            preferred subtree to read from
         */
        public Reader(MicroKernel mk, int preferred) {
            this.mk = mk;
            this.preferred = preferred;
            this.random = new Random(hashCode());
        }

        @Override
        public String call() throws Exception {
            for (int i = 0; i <= NB_READS; i++) {
                // read the preferred subtree
                int index = preferred;
                if (TREE_BRANCHING_FACTOR > 1
                        && random.nextDouble() >= PCT_LOCAL_READS) {
                    // read any other subtree
                    while (index == preferred) {
                        index = random.nextInt(TREE_BRANCHING_FACTOR);
                    }
                }
                // generate random path in that subtree
                String path = generatePath(index);
                // random depth
                int randomDepth = getRandomDepth();
                mk.getNodes(path, null, randomDepth, 0, -1, null);
            }
            return null;
        }

        private int getRandomDepth() {
            int depth = 0;
            while (random.nextDouble() > 0.75) {
                depth++;
            }
            return depth;
        }

        /**
         * Generates and returns the absolute path to a random node in the
         * subtree with specified index.
         */
        private String generatePath(int index) {
            String prefix = TreeCommitter.NODE_PREFIX;
            String path = prefix + index;
            // compute probability of a node: 1 / nb of nodes in the subtree
            double nb = (Math.pow(TREE_BRANCHING_FACTOR, TREE_HEIGHT) - 1)
                    / (TREE_BRANCHING_FACTOR - 1);
            double nodeProbability = 1.0 / nb;
            // loop through levels of the subtree
            for (int i = 0; i < TREE_HEIGHT - 1; i++) {
                // probability of picking a node on this level: probability of a
                // node * nb of nodes on that level
                double probability = nodeProbability
                        * Math.pow(TREE_BRANCHING_FACTOR, i);
                if (random.nextDouble() < probability) {
                    break;
                }
                // choose random node on that level
                path += "/" + prefix + random.nextInt(TREE_BRANCHING_FACTOR);
            }
            return "/" + path;
        }

    }

}