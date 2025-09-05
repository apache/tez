package org.apache.tez.examples;

import java.io.IOException;

import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;

import org.slf4j.Logger;

public abstract class ExampleBase {
    /**
     * Validate the arguments
     *
     * @param otherArgs arguments, if any
     * @return Zero indicates success, non-zero indicates failure
     */
    protected abstract int validateArgs(String[] otherArgs);

    /**
     * Print usage instructions for this example
     */
    protected abstract void printUsage();

    /**
     * Create and execute the actual DAG for the example
     *
     * @param args      arguments for execution
     * @param tezConf   the tez configuration instance to be used while processing the DAG
     * @param tezClient the tez client instance to use to run the DAG if any custom monitoring is
     *                  required. Otherwise the utility method {@link #runDag(DAG,
     *                  boolean, Logger)} should be used
     * @return Zero indicates success, non-zero indicates failure
     * @throws IOException
     * @throws TezException
     */
    protected abstract int runJob(String[] args, TezConfiguration tezConf,
                                  TezClient tezClient) throws Exception;
}
