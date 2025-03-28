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

package org.apache.tez.auxservices;

import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.DiskChecker;
import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import javax.crypto.SecretKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.ReadaheadPool;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.proto.ShuffleHandlerRecoveryProtos.JobShuffleInfoProto;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.security.SecureShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.api.ShuffleHandlerError;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleHeader;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.utils.LeveldbIterator;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.fusesource.leveldbjni.JniDBFactory;
import org.fusesource.leveldbjni.internal.NativeDB;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Logger;
import org.iq80.leveldb.Options;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.GlobalEventExecutor;

import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;

public class ShuffleHandler extends AuxiliaryService {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ShuffleHandler.class);
  private static final org.slf4j.Logger AUDITLOG =
      LoggerFactory.getLogger(ShuffleHandler.class.getName()+".audit");
  public static final String SHUFFLE_MANAGE_OS_CACHE = "tez.shuffle.manage.os.cache";
  public static final boolean DEFAULT_SHUFFLE_MANAGE_OS_CACHE = true;

  public static final String SHUFFLE_READAHEAD_BYTES = "tez.shuffle.readahead.bytes";
  public static final int DEFAULT_SHUFFLE_READAHEAD_BYTES = 4 * 1024 * 1024;
  public static final String USERCACHE = "usercache";
  public static final String APPCACHE = "appcache";

  // pattern to identify errors related to the client closing the socket early
  // idea borrowed from Netty SslHandler
  private static final Pattern IGNORABLE_ERROR_MESSAGE = Pattern.compile(
      "^.*(?:connection.*reset|connection.*closed|broken.*pipe).*$",
      Pattern.CASE_INSENSITIVE);

  private static final String STATE_DB_NAME = "tez_shuffle_state";
  private static final String STATE_DB_SCHEMA_VERSION_KEY = "shuffle-schema-version";
  protected static final Version CURRENT_VERSION_INFO =
      Version.newInstance(1, 0);

  private static final String DATA_FILE_NAME = "file.out";
  private static final String INDEX_FILE_NAME = "file.out.index";

  private int port;
  private NioEventLoopGroup bossGroup;
  private NioEventLoopGroup workerGroup;
  private final ChannelGroup accepted = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private int sslFileBufferSize;

  // pipeline items
  private Shuffle SHUFFLE;
  private SSLFactory sslFactory;

  /**
   * Should the shuffle use posix_fadvise calls to manage the OS cache during
   * sendfile
   */
  private boolean manageOsCache;
  private int readaheadLength;
  private int maxShuffleConnections;
  private int shuffleBufferSize;
  private boolean shuffleTransferToAllowed;
  private int maxSessionOpenFiles;
  private ReadaheadPool readaheadPool = ReadaheadPool.getInstance();

  private Map<String,String> userRsrc;
  private JobTokenSecretManager secretManager;

  private DB stateDb = null;

  public static final String TEZ_SHUFFLE_SERVICEID =
      "tez_shuffle";

  public static final String SHUFFLE_PORT_CONFIG_KEY = "tez.shuffle.port";
  public static final int DEFAULT_SHUFFLE_PORT = 13563;

  public static final String SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED =
      "tez.shuffle.connection-keep-alive.enable";
  public static final boolean DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED = false;

  public static final String SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT =
      "tez.shuffle.connection-keep-alive.timeout";
  public static final int DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT = 5; //seconds

  public static final String SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE =
      "tez.shuffle.mapoutput-info.meta.cache.size";
  public static final int DEFAULT_SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE =
      1000;

  public static final String CONNECTION_CLOSE = "close";

  public static final String SHUFFLE_SSL_ENABLED_KEY = "tez.shuffle.ssl.enabled";
  public static final boolean SHUFFLE_SSL_ENABLED_DEFAULT = false;

  public static final String SUFFLE_SSL_FILE_BUFFER_SIZE_KEY =
    "tez.shuffle.ssl.file.buffer.size";

  public static final int DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE = 60 * 1024;

  public static final String MAX_SHUFFLE_CONNECTIONS = "tez.shuffle.max.connections";
  public static final int DEFAULT_MAX_SHUFFLE_CONNECTIONS = 0; // 0 implies no limit

  public static final String MAX_SHUFFLE_THREADS = "tez.shuffle.max.threads";
  // 0 implies Netty default of 2 * number of available processors
  public static final int DEFAULT_MAX_SHUFFLE_THREADS = 0;

  public static final String SHUFFLE_BUFFER_SIZE =
      "tez.shuffle.transfer.buffer.size";
  public static final int DEFAULT_SHUFFLE_BUFFER_SIZE = 128 * 1024;

  public static final String  SHUFFLE_TRANSFERTO_ALLOWED =
      "tez.shuffle.transferTo.allowed";
  public static final boolean DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED = true;
  public static final boolean WINDOWS_DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED =
      false;
  private static final String TIMEOUT_HANDLER = "timeout";
  /* the maximum number of files a single GET request can
   open simultaneously during shuffle
   */
  public static final String SHUFFLE_MAX_SESSION_OPEN_FILES =
      "tez.shuffle.max.session-open-files";
  public static final int DEFAULT_SHUFFLE_MAX_SESSION_OPEN_FILES = 3;
  public static final String SHUFFLE_LISTEN_QUEUE_SIZE = "tez.shuffle.listen.queue.size";
  public static final int DEFAULT_SHUFFLE_LISTEN_QUEUE_SIZE = 128;

  boolean connectionKeepAliveEnabled = false;
  private int connectionKeepAliveTimeOut;
  private int mapOutputMetaInfoCacheSize;

  @Metrics(about="Shuffle output metrics", context="mapred", name="tez")
  static class ShuffleMetrics implements ChannelFutureListener {
    @Metric("Shuffle output in bytes")
        MutableCounterLong shuffleOutputBytes;
    @Metric("# of failed shuffle outputs")
        MutableCounterInt shuffleOutputsFailed;
    @Metric("# of succeeded shuffle outputs")
        MutableCounterInt shuffleOutputsOK;
    @Metric("# of current shuffle connections")
        MutableGaugeInt shuffleConnections;

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (future.isSuccess()) {
        shuffleOutputsOK.incr();
      } else {
        shuffleOutputsFailed.incr();
      }
      shuffleConnections.decr();
    }
  }

  final ShuffleMetrics metrics;

  class ReduceMapFileCount implements ChannelFutureListener {

    private ReduceContext reduceContext;

    public ReduceMapFileCount(ReduceContext rc) {
      this.reduceContext = rc;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      Channel ch = future.channel();
      if (!future.isSuccess()) {
        ch.close();
        return;
      }
      int waitCount = this.reduceContext.getMapsToWait().decrementAndGet();
      if (waitCount == 0) {
        LOG.debug("Finished with all map outputs");
        /*
         * LastHttpContent.EMPTY_LAST_CONTENT can only be written when there are no remaining maps to send,
         * this is the only time we can finish the HTTP response.
         */
        ch.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
        metrics.operationComplete(future);
        // Let the idle timer handler close keep-alive connections
        if (reduceContext.getKeepAlive()) {
          ChannelPipeline pipeline = ch.pipeline();
          TimeoutHandler timeoutHandler =
              (TimeoutHandler) pipeline.get(TIMEOUT_HANDLER);
          timeoutHandler.setEnabledTimeout(true);
        } else {
          ch.close();
        }
      } else {
        SHUFFLE.sendMap(reduceContext);
      }
    }
  }

  /**
   * Maintain parameters per channelRead() Netty context.
   * Allows sendMapOutput calls from operationComplete()
   */
  private static class ReduceContext {

    private List<String> mapIds;
    private AtomicInteger mapsToWait;
    private AtomicInteger mapsToSend;
    private Range reduceRange;
    private ChannelHandlerContext ctx;
    private String user;
    private Map<String, Shuffle.MapOutputInfo> infoMap;
    private String jobId;
    private String dagId;
    private final boolean keepAlive;

    public ReduceContext(List<String> mapIds, Range reduceRange,
                         ChannelHandlerContext context, String usr,
                         Map<String, Shuffle.MapOutputInfo> mapOutputInfoMap,
                         String jobId, String dagId, boolean keepAlive) {

      this.mapIds = mapIds;
      this.reduceRange = reduceRange;
      this.dagId = dagId;
      /**
      * Atomic count for tracking the no. of map outputs that are yet to
      * complete. Multiple futureListeners' operationComplete() can decrement
      * this value asynchronously. It is used to decide when the channel should
      * be closed.
      */
      this.mapsToWait = new AtomicInteger(mapIds.size());
      /**
      * Atomic count for tracking the no. of map outputs that have been sent.
      * Multiple sendMap() calls can increment this value
      * asynchronously. Used to decide which mapId should be sent next.
      */
      this.mapsToSend = new AtomicInteger(0);
      this.ctx = context;
      this.user = usr;
      this.infoMap = mapOutputInfoMap;
      this.jobId = jobId;
      this.keepAlive = keepAlive;
    }

    public Range getReduceRange() {
      return reduceRange;
    }

    public ChannelHandlerContext getCtx() {
      return ctx;
    }

    public String getUser() {
      return user;
    }

    public Map<String, Shuffle.MapOutputInfo> getInfoMap() {
      return infoMap;
    }

    public String getJobId() {
      return jobId;
    }

    public List<String> getMapIds() {
      return mapIds;
    }

    public AtomicInteger getMapsToSend() {
      return mapsToSend;
    }

    public AtomicInteger getMapsToWait() {
      return mapsToWait;
    }

    public boolean getKeepAlive() {
      return keepAlive;
    }
  }

  ShuffleHandler(MetricsSystem ms) {
    super(TEZ_SHUFFLE_SERVICEID);
    metrics = ms.register(new ShuffleMetrics());
  }

  public ShuffleHandler() {
    this(DefaultMetricsSystem.instance());
  }

  /**
   * Serialize the shuffle port into a ByteBuffer for use later on.
   * @param port the port to be sent to the ApplciationMaster
   * @return the serialized form of the port.
   */
  public static ByteBuffer serializeMetaData(int port) throws IOException {
    //TODO these bytes should be versioned
    DataOutputBuffer portDob = new DataOutputBuffer();
    portDob.writeInt(port);
    ByteBuffer buf = ByteBuffer.wrap(portDob.getData(), 0, portDob.getLength());
    portDob.close();
    return buf;
  }

  /**
   * A helper function to deserialize the metadata returned by ShuffleHandler.
   * @param meta the metadata returned by the ShuffleHandler
   * @return the port the Shuffle Handler is listening on to serve shuffle data.
   */
  public static int deserializeMetaData(ByteBuffer meta) throws IOException {
    //TODO this should be returning a class not just an int
    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(meta);
    int port = in.readInt();
    in.close();
    return port;
  }

  /**
   * A helper function to serialize the JobTokenIdentifier to be sent to the
   * ShuffleHandler as ServiceData.
   * @param jobToken the job token to be used for authentication of
   * shuffle data requests.
   * @return the serialized version of the jobToken.
   */
  public static ByteBuffer serializeServiceData(Token<JobTokenIdentifier> jobToken) throws IOException {
    //TODO these bytes should be versioned
    DataOutputBuffer jobToken_dob = new DataOutputBuffer();
    jobToken.write(jobToken_dob);
    return ByteBuffer.wrap(jobToken_dob.getData(), 0, jobToken_dob.getLength());
  }

  static Token<JobTokenIdentifier> deserializeServiceData(ByteBuffer secret) throws IOException {
    DataInputByteBuffer in = new DataInputByteBuffer();
    in.reset(secret);
    Token<JobTokenIdentifier> jt = new Token<JobTokenIdentifier>();
    jt.readFields(in);
    return jt;
  }

  public int getPort() {
    return port;
  }

  @Override
  public void initializeApplication(ApplicationInitializationContext context) {

    String user = context.getUser();
    ApplicationId appId = context.getApplicationId();
    ByteBuffer secret = context.getApplicationDataForService();
    // TODO these bytes should be versioned
    try {
      Token<JobTokenIdentifier> jt = deserializeServiceData(secret);
       // TODO: Once SHuffle is out of NM, this can use MR APIs
      JobID jobId = new JobID(Long.toString(appId.getClusterTimestamp()), appId.getId());
      recordJobShuffleInfo(jobId, user, jt);
    } catch (IOException e) {
      LOG.error("Error during initApp", e);
      // TODO add API to AuxiliaryServices to report failures
    }
  }

  @Override
  public void stopApplication(ApplicationTerminationContext context) {
    ApplicationId appId = context.getApplicationId();
    JobID jobId = new JobID(Long.toString(appId.getClusterTimestamp()), appId.getId());
    try {
      removeJobShuffleInfo(jobId);
    } catch (IOException e) {
      LOG.error("Error during stopApp", e);
      // TODO add API to AuxiliaryServices to report failures
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    manageOsCache = conf.getBoolean(SHUFFLE_MANAGE_OS_CACHE,
        DEFAULT_SHUFFLE_MANAGE_OS_CACHE);

    readaheadLength = conf.getInt(SHUFFLE_READAHEAD_BYTES,
        DEFAULT_SHUFFLE_READAHEAD_BYTES);

    maxShuffleConnections = conf.getInt(MAX_SHUFFLE_CONNECTIONS,
                                        DEFAULT_MAX_SHUFFLE_CONNECTIONS);
    int maxShuffleThreads = conf.getInt(MAX_SHUFFLE_THREADS,
                                        DEFAULT_MAX_SHUFFLE_THREADS);
    if (maxShuffleThreads == 0) {
      maxShuffleThreads = 2 * Runtime.getRuntime().availableProcessors();
    }

    shuffleBufferSize = conf.getInt(SHUFFLE_BUFFER_SIZE,
                                    DEFAULT_SHUFFLE_BUFFER_SIZE);

    shuffleTransferToAllowed = conf.getBoolean(SHUFFLE_TRANSFERTO_ALLOWED,
         (Shell.WINDOWS)?WINDOWS_DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED:
                         DEFAULT_SHUFFLE_TRANSFERTO_ALLOWED);

    maxSessionOpenFiles = conf.getInt(SHUFFLE_MAX_SESSION_OPEN_FILES,
        DEFAULT_SHUFFLE_MAX_SESSION_OPEN_FILES);

    final String BOSS_THREAD_NAME_PREFIX = "Tez Shuffle Handler Boss #";
    AtomicInteger bossThreadCounter = new AtomicInteger(0);
    bossGroup = new NioEventLoopGroup(1, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, BOSS_THREAD_NAME_PREFIX + bossThreadCounter.incrementAndGet());
      }
    });

    final String WORKER_THREAD_NAME_PREFIX = "Tez Shuffle Handler Worker #";
    AtomicInteger workerThreadCounter = new AtomicInteger(0);
    workerGroup = new NioEventLoopGroup(maxShuffleThreads, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, WORKER_THREAD_NAME_PREFIX + workerThreadCounter.incrementAndGet());
      }
    });
    port = conf.getInt(SHUFFLE_PORT_CONFIG_KEY, DEFAULT_SHUFFLE_PORT);
    super.serviceInit(new YarnConfiguration(conf));
  }

  // TODO change AbstractService to throw InterruptedException
  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    userRsrc = new ConcurrentHashMap<String,String>();
    secretManager = new JobTokenSecretManager(conf);
    recoverState(conf);
    ServerBootstrap bootstrap = new ServerBootstrap()
        .channel(NioServerSocketChannel.class)
        .group(bossGroup, workerGroup)
        .localAddress(port)
        .option(ChannelOption.SO_BACKLOG,
            conf.getInt(SHUFFLE_LISTEN_QUEUE_SIZE, DEFAULT_SHUFFLE_LISTEN_QUEUE_SIZE))
        .childOption(ChannelOption.SO_KEEPALIVE, true);
    initPipeline(bootstrap, conf);
    Channel ch = bootstrap.bind().sync().channel();
    accepted.add(ch);

    // setup port
    port = ((InetSocketAddress)ch.localAddress()).getPort();
    conf.set(SHUFFLE_PORT_CONFIG_KEY, Integer.toString(port));
    SHUFFLE.setPort(port);
    LOG.info(getName() + " listening on port " + port);

    super.serviceStart();

    sslFileBufferSize = conf.getInt(SUFFLE_SSL_FILE_BUFFER_SIZE_KEY,
                                    DEFAULT_SUFFLE_SSL_FILE_BUFFER_SIZE);
    connectionKeepAliveEnabled =
        conf.getBoolean(SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED,
          DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED);
    connectionKeepAliveTimeOut =
        Math.max(1, conf.getInt(SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT,
          DEFAULT_SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT));
    mapOutputMetaInfoCacheSize =
        Math.max(1, conf.getInt(SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE,
          DEFAULT_SHUFFLE_MAPOUTPUT_META_INFO_CACHE_SIZE));
  }

  private void initPipeline(ServerBootstrap bootstrap, Configuration conf) throws Exception {
    SHUFFLE = getShuffle(conf);
    if (conf.getBoolean(SHUFFLE_SSL_ENABLED_KEY, SHUFFLE_SSL_ENABLED_DEFAULT)) {
      LOG.info("Encrypted shuffle is enabled.");
      sslFactory = new SSLFactory(SSLFactory.Mode.SERVER, conf);
      sslFactory.init();
    }

    ChannelInitializer<NioSocketChannel> channelInitializer =
        new ChannelInitializer<NioSocketChannel>() {
          @Override
      public void initChannel(NioSocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        if (sslFactory != null) {
          pipeline.addLast("ssl", new SslHandler(sslFactory.createSSLEngine()));
        }
        if (LOG.isDebugEnabled()) {
          pipeline.addLast("loggingHandler", new LoggingHandler(LogLevel.DEBUG));
        }
        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("aggregator", new HttpObjectAggregator(1 << 16));
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("chunking", new ChunkedWriteHandler());
        pipeline.addLast("shuffle", SHUFFLE);
        pipeline.addLast("idle", new IdleStateHandler(0, connectionKeepAliveTimeOut, 0));
        pipeline.addLast(TIMEOUT_HANDLER, new TimeoutHandler());
      }
    };
    bootstrap.childHandler(channelInitializer);
  }

  private void destroyPipeline() {
    if (sslFactory != null) {
      sslFactory.destroy();
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    accepted.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
    if (bossGroup != null) {
      bossGroup.shutdownGracefully();
    }
    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
    }
    destroyPipeline();
    if (stateDb != null) {
      stateDb.close();
    }
    super.serviceStop();
  }

  @Override
  public synchronized ByteBuffer getMetaData() {
    try {
      return serializeMetaData(port);
    } catch (IOException e) {
      LOG.error("Error during getMeta", e);
      // TODO add API to AuxiliaryServices to report failures
      return null;
    }
  }

  protected Shuffle getShuffle(Configuration conf) {
    return new Shuffle(conf);
  }

  protected JobTokenSecretManager getSecretManager() {
    return secretManager;
  }

  private void recoverState(Configuration conf) throws IOException {
    Path recoveryRoot = getRecoveryPath();
    if (recoveryRoot != null) {
      startStore(recoveryRoot);
      Pattern jobPattern = Pattern.compile(JobID.JOBID_REGEX);
      LeveldbIterator iter = null;
      try {
        iter = new LeveldbIterator(stateDb);
        iter.seek(bytes(JobID.JOB));
        while (iter.hasNext()) {
          Map.Entry<byte[],byte[]> entry = iter.next();
          String key = asString(entry.getKey());
          if (!jobPattern.matcher(key).matches()) {
            break;
          }
          recoverJobShuffleInfo(key, entry.getValue());
        }
      } catch (DBException e) {
        throw new IOException("Database error during recovery", e);
      } finally {
        if (iter != null) {
          iter.close();
        }
      }
    }
  }

  private void startStore(Path recoveryRoot) throws IOException {
    Options options = new Options();
    options.createIfMissing(false);
    options.logger(new LevelDBLogger());
    Path dbPath = new Path(recoveryRoot, STATE_DB_NAME);
    LOG.info("Using state database at " + dbPath + " for recovery");
    File dbfile = new File(dbPath.toString());
    try {
      stateDb = JniDBFactory.factory.open(dbfile, options);
    } catch (NativeDB.DBException e) {
      if (e.isNotFound() || e.getMessage().contains(" does not exist ")) {
        LOG.info("Creating state database at " + dbfile);
        options.createIfMissing(true);
        try {
          stateDb = JniDBFactory.factory.open(dbfile, options);
          storeVersion();
        } catch (DBException dbExc) {
          throw new IOException("Unable to create state store", dbExc);
        }
      } else {
        throw e;
      }
    }
    checkVersion();
  }

  @VisibleForTesting
  Version loadVersion() throws IOException {
    byte[] data = stateDb.get(bytes(STATE_DB_SCHEMA_VERSION_KEY));
    // if version is not stored previously, treat it as CURRENT_VERSION_INFO.
    if (data == null || data.length == 0) {
      return getCurrentVersion();
    }
    Version version =
        new VersionPBImpl(VersionProto.parseFrom(data));
    return version;
  }

  private void storeSchemaVersion(Version version) throws IOException {
    String key = STATE_DB_SCHEMA_VERSION_KEY;
    byte[] data =
        ((VersionPBImpl) version).getProto().toByteArray();
    try {
      stateDb.put(bytes(key), data);
    } catch (DBException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  private void storeVersion() throws IOException {
    storeSchemaVersion(CURRENT_VERSION_INFO);
  }

  // Only used for test
  @VisibleForTesting
  void storeVersion(Version version) throws IOException {
    storeSchemaVersion(version);
  }

  protected Version getCurrentVersion() {
    return CURRENT_VERSION_INFO;
  }

  /**
   * 1) Versioning scheme: major.minor. For e.g. 1.0, 1.1, 1.2...1.25, 2.0 etc.
   * 2) Any incompatible change of DB schema is a major upgrade, and any
   *    compatible change of DB schema is a minor upgrade.
   * 3) Within a minor upgrade, say 1.1 to 1.2:
   *    overwrite the version info and proceed as normal.
   * 4) Within a major upgrade, say 1.2 to 2.0:
   *    throw exception and indicate user to use a separate upgrade tool to
   *    upgrade shuffle info or remove incompatible old state.
   */
  private void checkVersion() throws IOException {
    Version loadedVersion = loadVersion();
    LOG.info("Loaded state DB schema version info " + loadedVersion);
    if (loadedVersion.equals(getCurrentVersion())) {
      return;
    }
    if (loadedVersion.isCompatibleTo(getCurrentVersion())) {
      LOG.info("Storing state DB schedma version info " + getCurrentVersion());
      storeVersion();
    } else {
      throw new IOException(
        "Incompatible version for state DB schema: expecting DB schema version "
            + getCurrentVersion() + ", but loading version " + loadedVersion);
    }
  }

  private void addJobToken(JobID jobId, String user,
      Token<JobTokenIdentifier> jobToken) {
    userRsrc.put(jobId.toString(), user);
    getSecretManager().addTokenForJob(jobId.toString(), jobToken);
    LOG.info("Added token for " + jobId.toString());
  }

  private void recoverJobShuffleInfo(String jobIdStr, byte[] data)
      throws IOException {
    JobID jobId;
    try {
      jobId = JobID.forName(jobIdStr);
    } catch (IllegalArgumentException e) {
      throw new IOException("Bad job ID " + jobIdStr + " in state store", e);
    }

    JobShuffleInfoProto proto = JobShuffleInfoProto.parseFrom(data);
    String user = proto.getUser();
    TokenProto tokenProto = proto.getJobToken();
    Token<JobTokenIdentifier> jobToken = new Token<JobTokenIdentifier>(
        tokenProto.getIdentifier().toByteArray(),
        tokenProto.getPassword().toByteArray(),
        new Text(tokenProto.getKind()), new Text(tokenProto.getService()));
    addJobToken(jobId, user, jobToken);
  }

  private void recordJobShuffleInfo(JobID jobId, String user,
      Token<JobTokenIdentifier> jobToken) throws IOException {
    if (stateDb != null) {
      // Discover type instead of assuming ByteString to allow for shading.
      TokenProto tokenProto = TokenProto.newBuilder()
          .setIdentifier(TokenProto.getDefaultInstance().getIdentifier().copyFrom(jobToken.getIdentifier()))
          .setPassword(TokenProto.getDefaultInstance().getPassword().copyFrom(jobToken.getPassword()))
          .setKind(jobToken.getKind().toString())
          .setService(jobToken.getService().toString())
          .build();
      JobShuffleInfoProto proto = JobShuffleInfoProto.newBuilder()
          .setUser(user).setJobToken(tokenProto).build();
      try {
        stateDb.put(bytes(jobId.toString()), proto.toByteArray());
      } catch (DBException e) {
        throw new IOException("Error storing " + jobId, e);
      }
    }
    addJobToken(jobId, user, jobToken);
  }

  private void removeJobShuffleInfo(JobID jobId) throws IOException {
    String jobIdStr = jobId.toString();
    getSecretManager().removeTokenForJob(jobIdStr);
    userRsrc.remove(jobIdStr);
    if (stateDb != null) {
      try {
        stateDb.delete(bytes(jobIdStr));
      } catch (DBException e) {
        throw new IOException("Unable to remove " + jobId
            + " from state store", e);
      }
    }
  }

  private static class LevelDBLogger implements Logger {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(LevelDBLogger.class);

    @Override
    public void log(String message) {
      LOG.info(message);
    }
  }

  static class TimeoutHandler extends ChannelDuplexHandler {

    private boolean enabledTimeout;

    void setEnabledTimeout(boolean enabledTimeout) {
      this.enabledTimeout = enabledTimeout;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
      if (evt instanceof IdleStateEvent) {
        IdleStateEvent e = (IdleStateEvent) evt;
        if (e.state() == IdleState.WRITER_IDLE && enabledTimeout) {
          ctx.channel().close();
        }
      }
    }
  }

  protected static class Range {
    final int first;
    final int last;

    Range(int first, int last) {
      this.first = first;
      this.last = last;
    }

    int getFirst() {
      return first;
    }

    int getLast() {
      return last;
    }

    @Override
    public String toString() {
      return "range: " + first + "-" + last;
    }
  }

  @Sharable
  class Shuffle extends ChannelInboundHandlerAdapter {

    private static final int MAX_WEIGHT = 10 * 1024 * 1024;
    private static final int EXPIRE_AFTER_ACCESS_MINUTES = 5;
    private static final int ALLOWED_CONCURRENCY = 16;
    private final Configuration conf;
    private final IndexCache indexCache;
    private int port;
    private final LoadingCache<AttemptPathIdentifier, AttemptPathInfo> pathCache =
      CacheBuilder.newBuilder().expireAfterAccess(EXPIRE_AFTER_ACCESS_MINUTES,
      TimeUnit.MINUTES).softValues().concurrencyLevel(ALLOWED_CONCURRENCY).
      removalListener(
          new RemovalListener<AttemptPathIdentifier, AttemptPathInfo>() {
            @Override
            public void onRemoval(RemovalNotification<AttemptPathIdentifier,
                AttemptPathInfo> notification) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("PathCache Eviction: " + notification.getKey() +
                    ", Reason=" + notification.getCause());
              }
            }
          }
      ).maximumWeight(MAX_WEIGHT).weigher(
          new Weigher<AttemptPathIdentifier, AttemptPathInfo>() {
            @Override
            public int weigh(AttemptPathIdentifier key,
                AttemptPathInfo value) {
              return key.jobId.length() + key.user.length() +
                  key.attemptId.length()+
                  value.indexPath.toString().length() +
                  value.dataPath.toString().length();
            }
          }
      ).build(new CacheLoader<AttemptPathIdentifier, AttemptPathInfo>() {
        @Override
        public AttemptPathInfo load(AttemptPathIdentifier key) throws
            Exception {
          String base = getBaseLocation(key.jobId, key.dagId, key.user);
          String attemptBase = base + key.attemptId;
          Path indexFileName = getAuxiliaryLocalPathHandler()
              .getLocalPathForRead(attemptBase + "/" + INDEX_FILE_NAME);
          Path mapOutputFileName = getAuxiliaryLocalPathHandler()
              .getLocalPathForRead(attemptBase + "/" + DATA_FILE_NAME);

          LOG.debug("Loaded : {} via loader", key);
          return new AttemptPathInfo(indexFileName, mapOutputFileName);
        }
      });

    public Shuffle(Configuration conf) {
      this.conf = conf;
      indexCache = new IndexCache(conf);
      this.port = conf.getInt(SHUFFLE_PORT_CONFIG_KEY, DEFAULT_SHUFFLE_PORT);
    }

    public void setPort(int port) {
      this.port = port;
    }

    private List<String> splitMaps(List<String> mapq) {
      if (null == mapq) {
        return null;
      }
      final List<String> ret = new ArrayList<>();
      for (String s : mapq) {
        Collections.addAll(ret, s.split(","));
      }
      return ret;
    }

    private Range splitReduces(List<String> reduceq) {
      if (null == reduceq || reduceq.size() != 1) {
        return null;
      }
      String[] reduce = reduceq.get(0).split("-");
      int first = Integer.parseInt(reduce[0]);
      int last = first;
      if (reduce.length > 1) {
        last = Integer.parseInt(reduce[1]);
      }
      return new Range(first, last);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx)
        throws Exception {

      if ((maxShuffleConnections > 0) && (accepted.size() >= maxShuffleConnections)) {
        LOG.info(String.format("Current number of shuffle connections (%d) is " +
            "greater than or equal to the max allowed shuffle connections (%d)",
            accepted.size(), maxShuffleConnections));
        ctx.channel().close();
        return;
      }
      accepted.add(ctx.channel());
      super.channelActive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message)
        throws Exception {
      HttpRequest request = (HttpRequest) message;
      handleRequest(ctx, request);
    }

    private void handleRequest(ChannelHandlerContext ctx, HttpRequest request)
        throws IOException, Exception {
      if (request.getMethod() != GET) {
          sendError(ctx, METHOD_NOT_ALLOWED);
          return;
      }
      // Check whether the shuffle version is compatible
      if (!ShuffleHeader.DEFAULT_HTTP_HEADER_NAME.equals(
          request.headers().get(ShuffleHeader.HTTP_HEADER_NAME))
          || !ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION.equals(
              request.headers().get(ShuffleHeader.HTTP_HEADER_VERSION))) {
        sendError(ctx, "Incompatible shuffle request version", BAD_REQUEST);
        return;
      }
      final Map<String, List<String>> q = new QueryStringDecoder(request.getUri()).parameters();
      final List<String> keepAliveList = q.get("keepAlive");
      final List<String> dagCompletedQ = q.get("dagAction");
      final List<String> vertexCompletedQ = q.get("vertexAction");
      final List<String> taskAttemptFailedQ = q.get("taskAttemptAction");
      boolean keepAliveParam = false;
      if (keepAliveList != null && keepAliveList.size() == 1) {
        keepAliveParam = Boolean.parseBoolean(keepAliveList.get(0));
        LOG.debug("KeepAliveParam : {} : {}", keepAliveList, keepAliveParam);
      }
      final List<String> mapIds = splitMaps(q.get("map"));
      final Range reduceRange = splitReduces(q.get("reduce"));
      final List<String> jobQ = q.get("job");
      final List<String> dagIdQ = q.get("dag");
      final List<String> vertexIdQ = q.get("vertex");
      if (LOG.isDebugEnabled()) {
        LOG.debug("RECV: " + request.getUri() +
            "\n  mapId: " + mapIds +
            "\n  reduceId: " + reduceRange +
            "\n  jobId: " + jobQ +
            "\n  dagId: " + dagIdQ +
            "\n  keepAlive: " + keepAliveParam);
      }
      // If the request is for Dag Deletion, process the request and send OK.
      if (deleteDagDirectories(ctx.channel(), dagCompletedQ, jobQ, dagIdQ))  {
        return;
      }
      if (deleteVertexDirectories(ctx.channel(), vertexCompletedQ, jobQ, dagIdQ, vertexIdQ)) {
        return;
      }
      if (deleteTaskAttemptDirectories(ctx.channel(), taskAttemptFailedQ, jobQ, dagIdQ, mapIds)) {
        return;
      }
      if (mapIds == null || reduceRange == null || jobQ == null || dagIdQ == null) {
        sendError(ctx, "Required param job, dag, map and reduce", BAD_REQUEST);
        return;
      }
      if (jobQ.size() != 1) {
        sendError(ctx, "Too many job/reduce parameters", BAD_REQUEST);
        return;
      }

      // this audit log is disabled by default,
      // to turn it on please enable this audit log
      // on log4j.properties by uncommenting the setting
      if (AUDITLOG.isDebugEnabled()) {
        AUDITLOG.debug("shuffle for " + jobQ.get(0) + " mapper: " + mapIds
                         + " reducer: " + reduceRange);
      }
      String jobId;
      String dagId;
      try {
        jobId = jobQ.get(0);
        dagId = dagIdQ.get(0);
      } catch (NumberFormatException e) {
        sendError(ctx, "Bad reduce parameter", BAD_REQUEST);
        return;
      } catch (IllegalArgumentException e) {
        sendError(ctx, "Bad job parameter", BAD_REQUEST);
        return;
      }
      final String reqUri = request.getUri();
      if (null == reqUri) {
        // TODO? add upstream?
        sendError(ctx, FORBIDDEN);
        return;
      }
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
      try {
        verifyRequest(jobId, ctx, request, response,
            new URL("http", "", this.port, reqUri));
      } catch (IOException e) {
        LOG.warn("Shuffle failure ", e);
        sendError(ctx, e.getMessage(), UNAUTHORIZED);
        return;
      }

      Map<String, MapOutputInfo> mapOutputInfoMap =
          new HashMap<String, MapOutputInfo>();
      Channel ch = ctx.channel();
      ChannelPipeline pipeline = ch.pipeline();
      TimeoutHandler timeoutHandler = (TimeoutHandler)pipeline.get(TIMEOUT_HANDLER);
      timeoutHandler.setEnabledTimeout(false);
      String user = userRsrc.get(jobId);

      try {
        populateHeaders(mapIds, jobId, dagId, user, reduceRange,
          response, keepAliveParam, mapOutputInfoMap);
      } catch (DiskErrorException e) { // fatal error: fetcher should be aware of that
        LOG.error("Shuffle error in populating headers (fatal: DiskErrorException):", e);
        String errorMessage = getErrorMessage(e);
        // custom message, might be noticed by fetchers
        // it should reuse the current response object, as headers have been already set for it
        sendFakeShuffleHeaderWithError(ctx,
            ShuffleHandlerError.DISK_ERROR_EXCEPTION + ": " + errorMessage, response);
        return;
      } catch (IOException e) {
        ch.write(response);
        LOG.error("Shuffle error in populating headers :", e);
        String errorMessage = getErrorMessage(e);
        sendError(ctx, errorMessage, INTERNAL_SERVER_ERROR);
        return;
      }
      ch.write(response);
      //Initialize one ReduceContext object per channelRead call
      boolean keepAlive = keepAliveParam || connectionKeepAliveEnabled;
      ReduceContext reduceContext = new ReduceContext(mapIds, reduceRange, ctx,
          user, mapOutputInfoMap, jobId, dagId, keepAlive);
      for (int i = 0; i < Math.min(maxSessionOpenFiles, mapIds.size()); i++) {
        ChannelFuture nextMap = sendMap(reduceContext);
        if(nextMap == null) {
          return;
        }
      }
    }

    private boolean isNullOrEmpty(List<String> entries) {
      return entries == null || entries.isEmpty();
    }

    private boolean notEmptyAndContains(List<String> entries, String key) {
      if (entries == null || entries.isEmpty()) {
        return false;
      }
      return entries.get(0).contains(key);
    }

    private boolean deleteDagDirectories(Channel channel,
                                         List<String> dagCompletedQ, List<String> jobQ,
                                         List<String> dagIdQ) {
      if (jobQ == null || jobQ.isEmpty()) {
        return false;
      }
      if (notEmptyAndContains(dagCompletedQ,"delete") && !isNullOrEmpty(dagIdQ)) {
        String base = getDagLocation(jobQ.get(0), dagIdQ.get(0), userRsrc.get(jobQ.get(0)));
        try {
          FileContext lfc = FileContext.getLocalFSFileContext();
          for(Path dagPath : getAuxiliaryLocalPathHandler().getAllLocalPathsForRead(base)) {
            lfc.delete(dagPath, true);
          }
        } catch (IOException e) {
          LOG.warn("Encountered exception during dag delete "+ e);
        }
        channel.writeAndFlush(new DefaultHttpResponse(HTTP_1_1, OK))
            .addListener(ChannelFutureListener.CLOSE);
        return true;
      }
      return false;
    }

    private boolean deleteVertexDirectories(Channel channel, List<String> vertexCompletedQ,
                                            List<String> jobQ, List<String> dagIdQ,
                                            List<String> vertexIdQ) {
      if (jobQ == null || jobQ.isEmpty()) {
        return false;
      }
      if (notEmptyAndContains(vertexCompletedQ, "delete") && !isNullOrEmpty(vertexIdQ)) {
        try {
          deleteTaskDirsOfVertex(jobQ.get(0), dagIdQ.get(0), vertexIdQ.get(0), userRsrc.get(jobQ.get(0)));
        } catch (IOException e) {
          LOG.warn("Encountered exception during vertex delete " + e);
        }
        channel.writeAndFlush(new DefaultHttpResponse(HTTP_1_1, OK))
                .addListener(ChannelFutureListener.CLOSE);
        return true;
      }
      return false;
    }

    private boolean deleteTaskAttemptDirectories(Channel channel, List<String> taskAttemptFailedQ,
                                            List<String> jobQ, List<String> dagIdQ, List<String> taskAttemptIdQ) {
      if (jobQ == null || jobQ.isEmpty()) {
        return false;
      }
      if (notEmptyAndContains(taskAttemptFailedQ,"delete") && !isNullOrEmpty(taskAttemptIdQ)) {
        for (String taskAttemptId : taskAttemptIdQ) {
          String baseStr = getBaseLocation(jobQ.get(0), dagIdQ.get(0), userRsrc.get(jobQ.get(0)));
          try {
            FileSystem fs = FileSystem.getLocal(conf).getRaw();
            for (Path basePath : getAuxiliaryLocalPathHandler().getAllLocalPathsForRead(baseStr)) {
              for (FileStatus fileStatus : fs.listStatus(basePath)) {
                Path taskAttemptPath = fileStatus.getPath();
                if (taskAttemptPath.getName().startsWith(taskAttemptId)) {
                  if (fs.delete(taskAttemptPath, true)) {
                    LOG.info("Deleted directory : " + taskAttemptPath);
                    // remove entry from IndexCache
                    indexCache.removeMap(taskAttemptPath.getName());
                    break;
                  }
                }
              }
            }
          } catch (IOException e) {
            LOG.warn("Encountered exception during failed task attempt delete " + e);
          }
        }
        channel.writeAndFlush(new DefaultHttpResponse(HTTP_1_1, OK))
                .addListener(ChannelFutureListener.CLOSE);
        return true;
      }
      return false;
    }

    /**
     * Calls sendMapOutput for the mapId pointed by ReduceContext.mapsToSend
     * and increments it. This method is first called by channelRead()
     * maxSessionOpenFiles times and then on the completion of every
     * sendMapOutput operation. This limits the number of open files on a node,
     * which can get really large(exhausting file descriptors on the NM) if all
     * sendMapOutputs are called in one go, as was done previous to this change.
     * @param reduceContext used to call sendMapOutput with correct params.
     * @return the ChannelFuture of the sendMapOutput, can be null.
     */
    public ChannelFuture sendMap(ReduceContext reduceContext)
        throws Exception {
      ChannelFuture nextMap = null;
      if (reduceContext.getMapsToSend().get() <
          reduceContext.getMapIds().size()) {
        int nextIndex = reduceContext.getMapsToSend().getAndIncrement();
        String mapId = reduceContext.getMapIds().get(nextIndex);

        try {
          MapOutputInfo info = reduceContext.getInfoMap().get(mapId);

          if (info == null) {
            info = getMapOutputInfo(reduceContext.dagId, mapId, reduceContext.getReduceRange(),
                reduceContext.getJobId(),
                reduceContext.getUser());
          }

          nextMap = sendMapOutput(
              reduceContext.getCtx(),
              reduceContext.getCtx().channel(),
              reduceContext.getUser(), mapId,
              reduceContext.getReduceRange(), info);
          if (null == nextMap) {
            sendError(reduceContext.getCtx(), NOT_FOUND);
            return null;
          }
          nextMap.addListener(new ReduceMapFileCount(reduceContext));
        } catch (IOException e) {
          if (e instanceof DiskChecker.DiskErrorException) {
            LOG.error("Shuffle error :" + e);
          } else {
            LOG.error("Shuffle error :", e);
          }
          String errorMessage = getErrorMessage(e);
          sendError(reduceContext.getCtx(), errorMessage,
              INTERNAL_SERVER_ERROR);
          return null;
        }
      }
      return nextMap;
    }

    private String getErrorMessage(Throwable t) {
      StringBuffer sb = new StringBuffer(t.getMessage());
      while (t.getCause() != null) {
        sb.append(t.getCause().getMessage());
        t = t.getCause();
      }
      return sb.toString();
    }

    private String getBaseLocation(String jobId, String dagId, String user) {
      final String baseStr =
          getDagLocation(jobId, dagId, user) + "output" + Path.SEPARATOR;
      return baseStr;
    }

    /**
     * Delete shuffle data in task directories belonging to a vertex.
     */
    private void deleteTaskDirsOfVertex(String jobId, String dagId, String vertexId, String user) throws IOException {
      String baseStr = getBaseLocation(jobId, dagId, user);
      FileContext lfc = FileContext.getLocalFSFileContext();
      for(Path dagPath : getAuxiliaryLocalPathHandler().getAllLocalPathsForRead(baseStr)) {
        RemoteIterator<FileStatus> status = lfc.listStatus(dagPath);
        final JobID jobID = JobID.forName(jobId);
        String taskDirPrefix = String.format("attempt%s_%s_%s_",
            jobID.toString().replace("job", ""), dagId, vertexId);
        while (status.hasNext()) {
          FileStatus fileStatus = status.next();
          Path attemptPath = fileStatus.getPath();
          if (attemptPath.getName().startsWith(taskDirPrefix)) {
            if(lfc.delete(attemptPath, true)) {
              LOG.debug("deleted shuffle data in task directory: {}", attemptPath);
            }
          }
        }
      }
    }

    private String getDagLocation(String jobId, String dagId, String user) {
      final JobID jobID = JobID.forName(jobId);
      final ApplicationId appID =
          ApplicationId.newInstance(Long.parseLong(jobID.getJtIdentifier()),
              jobID.getId());
      final String dagStr =
          USERCACHE + Path.SEPARATOR + user + Path.SEPARATOR
              + APPCACHE + Path.SEPARATOR
              + appID.toString() + Path.SEPARATOR + Constants.DAG_PREFIX + dagId
              + Path.SEPARATOR;
      return dagStr;
    }

    protected MapOutputInfo getMapOutputInfo(String dagId, String mapId,
                                             Range reduceRange, String jobId,
                                             String user) throws IOException {
      AttemptPathInfo pathInfo;
      try {
        AttemptPathIdentifier identifier = new AttemptPathIdentifier(
            jobId, dagId, user, mapId);
        pathInfo = pathCache.get(identifier);
        LOG.debug("Retrieved pathInfo for {} check for corresponding loaded " +
            "messages to determine whether it was loaded or cached", identifier);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        } else {
          throw new RuntimeException(e.getCause());
        }
      }

      TezSpillRecord spillRecord =
          indexCache.getSpillRecord(mapId, pathInfo.indexPath, user);

      if (LOG.isDebugEnabled()) {
        LOG.debug("getMapOutputInfo: jobId=" + jobId + ", mapId=" + mapId +
            ",dataFile=" + pathInfo.dataPath + ", indexFile=" +
            pathInfo.indexPath);
      }

      MapOutputInfo outputInfo;
      if (reduceRange.first == reduceRange.last) {
        outputInfo = new MapOutputInfo(pathInfo.dataPath, spillRecord.getIndex(reduceRange.first), reduceRange);
      } else {

        outputInfo = new MapOutputInfo(pathInfo.dataPath, spillRecord, reduceRange);
      }
      return outputInfo;
    }

    protected void populateHeaders(List<String> mapIds, String jobId,
                                   String dagId, String user,
                                   Range reduceRange,
                                   HttpResponse response,
                                   boolean keepAliveParam,
                                   Map<String, MapOutputInfo> mapOutputInfoMap)
        throws IOException {

      long contentLength = 0;
      // Content-Length only needs calculated for keep-alive keep alive
      if (connectionKeepAliveEnabled || keepAliveParam) {
        contentLength = getContentLength(mapIds, jobId, dagId, user, reduceRange, mapOutputInfoMap);
      }

      // Now set the response headers.
      setResponseHeaders(response, keepAliveParam, contentLength);
    }

    long getContentLength(List<String> mapIds, String jobId, String dagId, String user, Range reduceRange, Map<String, MapOutputInfo> mapOutputInfoMap) throws IOException {
      long contentLength = 0;
      // Reduce count is written once per mapId
      int reduceCountVSize = WritableUtils.getVIntSize(reduceRange.getLast() - reduceRange.getFirst() + 1);
      for (String mapId : mapIds) {
        contentLength += reduceCountVSize;
        MapOutputInfo outputInfo = getMapOutputInfo(dagId, mapId, reduceRange, jobId, user);
        if (mapOutputInfoMap.size() < mapOutputMetaInfoCacheSize) {
          mapOutputInfoMap.put(mapId, outputInfo);
        }
        for (int reduce = reduceRange.getFirst(); reduce <= reduceRange.getLast(); reduce++) {
          TezIndexRecord indexRecord = outputInfo.getIndex(reduce);
          ShuffleHeader header =
              new ShuffleHeader(mapId, indexRecord.getPartLength(), indexRecord.getRawLength(), reduce);

          contentLength += header.writeLength();
          contentLength += indexRecord.getPartLength();
        }
      }
      return contentLength;
    }

    protected void setResponseHeaders(HttpResponse response, boolean keepAliveParam, long contentLength) {
      if (connectionKeepAliveEnabled || keepAliveParam) {
        response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(contentLength));
        response.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        response.headers().set(HttpHeaders.Values.KEEP_ALIVE, "timeout=" + connectionKeepAliveTimeOut);
        LOG.debug("Content Length in shuffle : {}", contentLength);
      } else {
        LOG.debug("Setting connection close header...");
        response.headers().set(HttpHeaders.Names.CONNECTION, CONNECTION_CLOSE);
      }
    }

    class MapOutputInfo {
      private final Path mapOutputFileName;
      private TezSpillRecord spillRecord;
      private TezIndexRecord indexRecord;
      private final Range reduceRange;

      MapOutputInfo(Path mapOutputFileName, TezIndexRecord indexRecord, Range reduceRange) {
        this.mapOutputFileName = mapOutputFileName;
        this.indexRecord = indexRecord;
        this.reduceRange = reduceRange;
      }

      MapOutputInfo(Path mapOutputFileName, TezSpillRecord spillRecord, Range reduceRange) {
        this.mapOutputFileName = mapOutputFileName;
        this.spillRecord = spillRecord;
        this.reduceRange = reduceRange;
      }

      TezIndexRecord getIndex(int index) {
        if (index < reduceRange.first || index > reduceRange.last) {
          throw new IllegalArgumentException("Reduce Index: " + index + " out of range for " + mapOutputFileName);
        }
        if (spillRecord != null) {
          return spillRecord.getIndex(index);
        } else {
          return indexRecord;
        }
      }

      public void finish() {
        spillRecord = null;
        indexRecord = null;
      }
    }

    protected void verifyRequest(String appid, ChannelHandlerContext ctx,
        HttpRequest request, HttpResponse response, URL requestUri)
        throws IOException {
      SecretKey tokenSecret = getSecretManager().retrieveTokenSecret(appid);
      if (null == tokenSecret) {
        LOG.info("Request for unknown token " + appid);
        throw new IOException("could not find jobid");
      }
      // string to encrypt
      String enc_str = SecureShuffleUtils.buildMsgFrom(requestUri);
      // hash from the fetcher
      String urlHashStr =
        request.headers().get(SecureShuffleUtils.HTTP_HEADER_URL_HASH);
      if (urlHashStr == null) {
        LOG.info("Missing header hash for " + appid);
        throw new IOException("fetcher cannot be authenticated");
      }
      if (LOG.isDebugEnabled()) {
        int len = urlHashStr.length();
        LOG.debug("verifying request. enc_str=" + enc_str + "; hash=..." +
            urlHashStr.substring(len-len/2, len-1));
      }
      // verify - throws exception
      SecureShuffleUtils.verifyReply(urlHashStr, enc_str, tokenSecret);
      // verification passed - encode the reply
      String reply =
        SecureShuffleUtils.generateHash(urlHashStr.getBytes(Charsets.UTF_8),
            tokenSecret);
      response.headers().set(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH, reply);
      // Put shuffle version into http header
      response.headers().set(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      response.headers().set(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      if (LOG.isDebugEnabled()) {
        int len = reply.length();
        LOG.debug("Fetcher request verfied. enc_str=" + enc_str + ";reply=" +
            reply.substring(len-len/2, len-1));
      }
    }

    protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx, Channel ch,
                                          String user, String mapId, Range reduceRange, MapOutputInfo outputInfo)
        throws IOException {
      TezIndexRecord firstIndex = null;
      TezIndexRecord lastIndex = null;

      DataOutputBuffer dobRange = new DataOutputBuffer();
      // Indicate how many record to be written
      WritableUtils.writeVInt(dobRange, reduceRange.getLast() - reduceRange.getFirst() + 1);
      ch.writeAndFlush(wrappedBuffer(dobRange.getData(), 0, dobRange.getLength()));
      for (int reduce = reduceRange.getFirst(); reduce <= reduceRange.getLast(); reduce++) {
        TezIndexRecord index = outputInfo.getIndex(reduce);
        // Records are only valid if they have a non-zero part length
        if (index.getPartLength() != 0) {
          if (firstIndex == null) {
            firstIndex = index;
          }
          lastIndex = index;
        }

        ShuffleHeader header = new ShuffleHeader(mapId, index.getPartLength(), index.getRawLength(), reduce);
        DataOutputBuffer dob = new DataOutputBuffer();
        header.write(dob);
        // Free the memory needed to store the spill and index records
        ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
      }
      outputInfo.finish();

      final long rangeOffset = firstIndex.getStartOffset();
      final long rangePartLength = lastIndex.getStartOffset() + lastIndex.getPartLength() - firstIndex.getStartOffset();
      final File spillFile = new File(outputInfo.mapOutputFileName.toString());
      RandomAccessFile spill;
      try {
        spill = SecureIOUtils.openForRandomRead(spillFile, "r", user, null);
      } catch (FileNotFoundException e) {
        LOG.info(spillFile + " not found");
        return null;
      }
      ChannelFuture writeFuture;
      if (ch.pipeline().get(SslHandler.class) == null) {
        final FadvisedFileRegion partition = new FadvisedFileRegion(spill,
            rangeOffset, rangePartLength, manageOsCache, readaheadLength,
            readaheadPool, spillFile.getAbsolutePath(),
            shuffleBufferSize, shuffleTransferToAllowed);
        writeFuture = ch.writeAndFlush(partition);
      } else {
        // HTTPS cannot be done with zero copy.
        final FadvisedChunkedFile chunk = new FadvisedChunkedFile(spill,
            rangeOffset, rangePartLength, sslFileBufferSize,
            manageOsCache, readaheadLength, readaheadPool,
            spillFile.getAbsolutePath());
        writeFuture = ch.writeAndFlush(chunk);
      }
      metrics.shuffleConnections.incr();
      metrics.shuffleOutputBytes.incr(rangePartLength); // optimistic
      return writeFuture;
    }

    protected void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
      sendError(ctx, "", status);
    }

    protected void sendError(ChannelHandlerContext ctx, String message, HttpResponseStatus status) {
      FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status);
      sendError(ctx, message, response);
    }

    protected void sendError(ChannelHandlerContext ctx, String message, FullHttpResponse response) {
      sendError(ctx, Unpooled.copiedBuffer(message, CharsetUtil.UTF_8), response);
    }

    private void sendFakeShuffleHeaderWithError(ChannelHandlerContext ctx, String message,
        HttpResponse response) throws IOException {
      FullHttpResponse fullResponse =
          new DefaultFullHttpResponse(response.getProtocolVersion(), response.getStatus());
      fullResponse.headers().set(response.headers());

      ShuffleHeader header = new ShuffleHeader(message, -1, -1, -1);
      DataOutputBuffer out = new DataOutputBuffer();
      header.write(out);

      sendError(ctx, wrappedBuffer(out.getData(), 0, out.getLength()), fullResponse);
    }

    protected void sendError(ChannelHandlerContext ctx, ByteBuf content,
        FullHttpResponse response) {
      response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
      // Put shuffle version into http header
      response.headers().set(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      response.headers().set(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      response.content().writeBytes(content);

      // Close the connection as soon as the error message is sent.
      ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
      /*
       * The general rule of thumb is that the party that accesses a reference-counted object last
       * is also responsible for the destruction of that reference-counted object.
       */
      content.release();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        throws Exception {
      if (cause instanceof TooLongFrameException) {
        sendError(ctx, BAD_REQUEST);
        return;
      } else if (cause instanceof IOException) {
        if (cause instanceof ClosedChannelException) {
          LOG.debug("Ignoring closed channel error", cause);
          return;
        }
        String message = String.valueOf(cause.getMessage());
        if (IGNORABLE_ERROR_MESSAGE.matcher(message).matches()) {
          LOG.debug("Ignoring client socket close", cause);
          return;
        }
      }

      LOG.error("Shuffle error: ", cause);
      if (ctx.channel().isActive()) {
        LOG.error("Shuffle error", cause);
        sendError(ctx, INTERNAL_SERVER_ERROR);
      }
    }
  }

  static class AttemptPathInfo {
    // TODO Change this over to just store local dir indices, instead of the
    // entire path. Far more efficient.
    private final Path indexPath;
    private final Path dataPath;

    public AttemptPathInfo(Path indexPath, Path dataPath) {
      this.indexPath = indexPath;
      this.dataPath = dataPath;
    }
  }

  static class AttemptPathIdentifier {
    private final String jobId;
    private final String dagId;
    private final String user;
    private final String attemptId;

    public AttemptPathIdentifier(String jobId, String dagID, String user,
                                 String attemptId) {
      this.jobId = jobId;
      this.dagId = dagID;
      this.user = user;
      this.attemptId = attemptId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      AttemptPathIdentifier that = (AttemptPathIdentifier) o;

      if (!attemptId.equals(that.attemptId)) {
        return false;
      }

      if (!jobId.equals(that.jobId)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = jobId.hashCode();
      result = 31 * result + attemptId.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "AttemptPathIdentifier{" +
          "jobId='" + jobId + '\'' +
          ", dagId='" + dagId + '\'' +
          ", user='" + user + '\'' +
          ", attemptId='" + attemptId + '\'' +
          '}';
    }
  }
}
