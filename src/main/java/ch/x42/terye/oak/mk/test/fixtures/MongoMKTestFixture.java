package ch.x42.terye.oak.mk.test.fixtures;

import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mongomk.impl.MongoConnection;
import org.apache.jackrabbit.mongomk.impl.MongoMicroKernel;
import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.blob.MongoGridFSBlobStore;

import com.mongodb.DB;

public class MongoMKTestFixture implements MicroKernelTestFixture {

    private static final String MONGODB_HOST = "localhost";
    private static final int MONGODB_PORT = 27017;
    private static final String MONGODB_DB = "mktest";

    private Set<MongoMicroKernel> mks;

    public MongoMKTestFixture() throws Exception {
        this.mks = new HashSet<MongoMicroKernel>();
    }

    @Override
    public MicroKernel createMicroKernel() throws Exception {
        MongoConnection connection = new MongoConnection(MONGODB_HOST,
                MONGODB_PORT, MONGODB_DB);
        DB mongoDB = connection.getDB();
        MongoNodeStore nodeStore = new MongoNodeStore(mongoDB);
        MongoGridFSBlobStore blobStore = new MongoGridFSBlobStore(mongoDB);
        MongoMicroKernel mk = new MongoMicroKernel(connection, nodeStore,
                blobStore);
        mks.add(mk);
        return mk;
    }

    @Override
    public void disposeMicroKernel(MicroKernel mk) throws Exception {
        if (mks.remove(mk)) {
            ((MongoMicroKernel) mk).dispose();
        }
    }

    @Override
    public void setUpBeforeTest() throws Exception {
        // nothing to do
    }

    @Override
    public void tearDownAfterTest() throws Exception {
        // dispose of remaining microkernels
        for (MongoMicroKernel mk : mks) {
            mk.dispose();
        }
        // drop collections
        new MongoConnection(MONGODB_HOST, MONGODB_PORT, MONGODB_DB).getDB()
                .dropDatabase();
        // clear list so that the microkernels can be gc'd
        mks.clear();
    }

    @Override
    public String toString() {
        return "MongoMicroKernel";
    }

}
