/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tez.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.engine.api.Input;
import org.apache.tez.engine.api.Master;
import org.apache.tez.engine.api.Output;
import org.apache.tez.engine.api.Processor;
import org.apache.tez.engine.api.Task;
import org.apache.tez.engine.lib.input.ShuffledMergedInput;
import org.apache.tez.engine.lib.output.OnFileSortedOutput;
import org.apache.tez.engine.runtime.InputFactory;
import org.apache.tez.engine.runtime.OutputFactory;
import org.apache.tez.engine.runtime.ProcessorFactory;
import org.apache.tez.engine.runtime.TaskFactory;
import org.apache.tez.engine.runtime.TezEngineFactory;
import org.apache.tez.engine.runtime.TezEngineFactoryImpl;
import org.apache.tez.mapreduce.input.SimpleInput;
import org.apache.tez.mapreduce.output.SimpleOutput;
import org.apache.tez.mapreduce.processor.map.MapProcessor;
import org.apache.tez.mapreduce.processor.reduce.ReduceProcessor;
import org.apache.tez.mapreduce.task.FinalTask;
import org.apache.tez.mapreduce.task.InitialTask;
import org.apache.tez.mapreduce.task.IntermediateTask;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.FactoryModuleBuilder;

public class TestTaskModules {
  
  private static final Log LOG = LogFactory.getLog(TestTaskModules.class);

  TezEngineTaskContext taskContext;
  JobConf job;
  
  @Before
  public void setUp() {
    taskContext = new TezEngineTaskContext(
        TezTestUtils.getMockTaskAttemptId(0, 0, 0, 0),
        "tez", "tez", "TODO_vertexName",
        TestInitialModule.class.getName(), null, null);
    job = new JobConf();
  }
  
  @Test
  @Ignore
  public void testInitialTask() throws Exception {
    Injector injector = Guice.createInjector(new TestInitialModule());
    TezEngineFactory factory = injector.getInstance(TezEngineFactory.class);
    Task t = factory.createTask(taskContext);
    t.initialize(job, null);
  }

  @Test
  @Ignore
  public void testIntermediateTask() throws Exception {
    Injector injector = Guice.createInjector(new TestIntermediateModule());
    TezEngineFactory factory = injector.getInstance(TezEngineFactory.class);
    Task t = factory.createTask(taskContext);
    t.initialize(job, null);
  }

  @Test
  @Ignore
  public void testFinalTask() throws Exception {
    Injector injector = Guice.createInjector(new TestFinalModule());
    TezEngineFactory factory = injector.getInstance(TezEngineFactory.class);
    Task task = factory.createTask(taskContext);
    LOG.info("task = " + task.getClass());
    task.initialize(job, null);
  }

  static class TestTask implements Task {

    private final Input[] ins;
    private final Output[] outs;
    private final Processor processor;
    
    @Inject
    public TestTask(
        @Assisted Processor processor, 
        @Assisted Input in, 
        @Assisted Output out) {
      this(processor, new Input[] {in},
          new Output[] {out});
    }

    @Inject
    public TestTask(
        Processor processor,
        Input[] ins,
        Output[] outs) {
      this.ins = ins;
      this.processor = processor;
      this.outs = outs;
    }

    @Override
    public void initialize(Configuration conf, Master master)
        throws IOException, InterruptedException {
      LOG.info("in = " + ins[0].getClass());
      LOG.info("processor = " + processor.getClass());
      LOG.info("out = " + outs[0].getClass());
    }

    @Override
    public Input[] getInputs() {
      return ins;
    }

    @Override
    public Output[] getOutputs() {
      return outs;
    }

    @Override
    public Processor getProcessor() {
      return processor;
    }

    @Override
    public void run() throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void close() throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      
    }
    
  }
  
  static class TestInitialModule extends InitialTask {

    @Override
    protected void configure() {    
      install(
          new FactoryModuleBuilder().implement(
              Input.class, SimpleInput.class).
          build(InputFactory.class)
          );
      install(
          new FactoryModuleBuilder().implement(
              Output.class, OnFileSortedOutput.class).
          build(OutputFactory.class)
          );
      install(
          new FactoryModuleBuilder().implement(
              Processor.class, MapProcessor.class).
          build(ProcessorFactory.class)
          );
      install(
          new FactoryModuleBuilder().implement(
              Task.class, TestTask.class).
          build(TaskFactory.class)
          );
      
      bind(TezEngineFactory.class).to(TezEngineFactoryImpl.class);
    }
    
  }
  

  static class TestIntermediateModule extends IntermediateTask {

    @Override
    protected void configure() {
      install(
          new FactoryModuleBuilder().implement(
              Input.class, ShuffledMergedInput.class).
          build(InputFactory.class)
          );
      install(
          new FactoryModuleBuilder().implement(
              Output.class, OnFileSortedOutput.class).
          build(OutputFactory.class)
          );
      install(
          new FactoryModuleBuilder().implement(
              Processor.class, ReduceProcessor.class).
          build(ProcessorFactory.class)
          );
      install(
          new FactoryModuleBuilder().implement(
              Task.class, TestTask.class).
          build(TaskFactory.class)
          );

      bind(TezEngineFactory.class).to(TezEngineFactoryImpl.class);
    }
    
  }
  

  static class TestFinalModule extends FinalTask {

    @Override
    protected void configure() {    
      install(
          new FactoryModuleBuilder().implement(
              Output.class, SimpleOutput.class).
          build(OutputFactory.class)
          );
      install(
          new FactoryModuleBuilder().implement(
              Input.class, ShuffledMergedInput.class).
          build(InputFactory.class)
          );
      install(
          new FactoryModuleBuilder().implement(
              Processor.class, ReduceProcessor.class).
          build(ProcessorFactory.class)
          );
      install(
          new FactoryModuleBuilder().implement(
              Task.class, TestTask.class).
          build(TaskFactory.class)
          );
      
      bind(TezEngineFactory.class).to(TezEngineFactoryImpl.class);
    }
    
  }
  
}
