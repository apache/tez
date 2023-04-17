package org.apache.tez.runtime.library.common;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RssShuffleFactory {
    private static final Logger LOG = LoggerFactory.getLogger(RssShuffleFactory.class);
    private static final String TEZ_RSS_RUNTIME_SHUFFLE_RSS_ENABLED = "tez.runtime.rss.enabled";
    private static final String TEZ_RSS_RUNTIME_IO_SORT_MB = "tez.rss.runtime.io.sort.mb";
    private static final String TEZ_RSS_CLIENT_SORT_MEMORY_USE_THREAD = "tez.rss.client.sort.memory.use.threshold";
    private static final String TEZ_RSS_CLIENT_MAX_BUFFER_SIZE = "tez.rss.client.max.buffer.size";
    private static final String TEZ_RSS_WRITER_BUFFER_SIZE = "tez.rss.writer.buffer.size";
    private static final String TEZ_RSS_CLIENT_MEMORY_THREAD = "tez.rss.client.memory.threshold";
    private static final String TEZ_RSS_CLIENT_SEND_THREAD = "tez.rss.client.send.threshold";
    private static final String TEZ_RSS_CLIENT_BATCH_TRIGGER_NUM = "tez.rss.client.batch.trigger.num";
    private static final String TEZ_RSS_STORAGE_TYPE = "tez.rss.storage.type";
    private static final String TEZ_RSS_CLIENT_SEND_CHECK_INTERVAL_MS = "tez.rss.client.send.check.interval.ms";
    private static final String TEZ_RSS_CLIENT_SEND_CHECK_TIMEOUT_MS = "tez.rss.client.send.check.timeout.ms";
    private static final String TEZ_RSS_CLIENT_BITMAP_NUM = "tez.rss.client.bitmap.num";
    private static final String HIVE_TEZ_LOG_LEVEL = "hive.tez.log.level";

    private static final boolean TEZ_RSS_RUNTIME_SHUFFLE_RSS_ENABLED_DEFAULT = false;

    public static boolean isRssEnabled(UserPayload userPayload) {
        try{
            Configuration conf = TezUtils.createConfFromUserPayload(userPayload);
            LOG.info("RssShuffleFactory.isRssEnabled ? {}", isRssEnabled(conf));
            return isRssEnabled(conf);
        }catch (IOException e){
            LOG.warn("Parse failed for user payload when check rss enabled or not!", e);
        }
        return false;
    }

    public static boolean isRssEnabled(Configuration conf) {
        return conf.getBoolean(TEZ_RSS_RUNTIME_SHUFFLE_RSS_ENABLED, TEZ_RSS_RUNTIME_SHUFFLE_RSS_ENABLED_DEFAULT);
    }

    public static class RssOrderedPartitionedKVOutput{
        public static final String outputClassName = "org.apache.tez.runtime.library.output.RssOrderedPartitionedKVOutput";

        private static final Set<String> confKeys = new HashSet<String>();

        static {
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_MAX_PER_BUFFER_SIZE_BYTES);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT);
            confKeys.add(TezConfiguration.TEZ_COUNTERS_MAX);
            confKeys.add(TezConfiguration.TEZ_COUNTERS_GROUP_NAME_MAX_LENGTH);
            confKeys.add(TezConfiguration.TEZ_COUNTERS_COUNTER_NAME_MAX_LENGTH);
            confKeys.add(TezConfiguration.TEZ_COUNTERS_MAX_GROUPS);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS);
            confKeys.add(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID);
            confKeys.add(
                    TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_PARTITIONED_KVWRITER_BUFFER_MERGE_PERCENT);
            confKeys.add(TEZ_RSS_RUNTIME_SHUFFLE_RSS_ENABLED);
            confKeys.add(TEZ_RSS_RUNTIME_IO_SORT_MB);
            confKeys.add(TEZ_RSS_CLIENT_SORT_MEMORY_USE_THREAD);
            confKeys.add(TEZ_RSS_CLIENT_MAX_BUFFER_SIZE);
            confKeys.add(TEZ_RSS_WRITER_BUFFER_SIZE);
            confKeys.add(TEZ_RSS_CLIENT_MEMORY_THREAD);
            confKeys.add(TEZ_RSS_CLIENT_SEND_THREAD);
            confKeys.add(TEZ_RSS_CLIENT_BATCH_TRIGGER_NUM);
            confKeys.add(TEZ_RSS_STORAGE_TYPE);
            confKeys.add(TEZ_RSS_CLIENT_SEND_CHECK_INTERVAL_MS);
            confKeys.add(TEZ_RSS_CLIENT_SEND_CHECK_TIMEOUT_MS);
            confKeys.add(TEZ_RSS_CLIENT_BITMAP_NUM);
            confKeys.add(HIVE_TEZ_LOG_LEVEL);
        }

        public static Set<String> getConfigurationKeySet() {
            return Collections.unmodifiableSet(confKeys);
        }
    }


    public static class RssUnOrderedPartitionedKVOutput{
        public static final String outputClassName = "org.apache.tez.runtime.library.output.RssUnorderedPartitionedKVOutput";
        private static final Set<String> confKeys = new HashSet<String>();

        static {
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_MAX_PER_BUFFER_SIZE_BYTES);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT);
            confKeys.add(TezConfiguration.TEZ_COUNTERS_MAX);
            confKeys.add(TezConfiguration.TEZ_COUNTERS_GROUP_NAME_MAX_LENGTH);
            confKeys.add(TezConfiguration.TEZ_COUNTERS_COUNTER_NAME_MAX_LENGTH);
            confKeys.add(TezConfiguration.TEZ_COUNTERS_MAX_GROUPS);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS);
            confKeys.add(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID);
            confKeys.add(
                    TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_PARTITIONED_KVWRITER_BUFFER_MERGE_PERCENT);

            confKeys.add(TEZ_RSS_RUNTIME_SHUFFLE_RSS_ENABLED);
            confKeys.add(TEZ_RSS_RUNTIME_IO_SORT_MB);
            confKeys.add(TEZ_RSS_CLIENT_SORT_MEMORY_USE_THREAD);
            confKeys.add(TEZ_RSS_CLIENT_MAX_BUFFER_SIZE);
            confKeys.add(TEZ_RSS_WRITER_BUFFER_SIZE);
            confKeys.add(TEZ_RSS_CLIENT_MEMORY_THREAD);
            confKeys.add(TEZ_RSS_CLIENT_SEND_THREAD);
            confKeys.add(TEZ_RSS_CLIENT_BATCH_TRIGGER_NUM);
            confKeys.add(TEZ_RSS_STORAGE_TYPE);
            confKeys.add(TEZ_RSS_CLIENT_SEND_CHECK_INTERVAL_MS);
            confKeys.add(TEZ_RSS_CLIENT_SEND_CHECK_TIMEOUT_MS);
            confKeys.add(TEZ_RSS_CLIENT_BITMAP_NUM);
            confKeys.add(HIVE_TEZ_LOG_LEVEL);
        }

        public static Set<String> getConfigurationKeySet() {
            return Collections.unmodifiableSet(confKeys);
        }
    }

    public static class RssUnorderedKVOutput{
        public static final String outputClassName = "org.apache.tez.runtime.library.output.RssUnorderedKVOutput";

        private static final Set<String> confKeys = new HashSet<String>();

        static {
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IFILE_READAHEAD_BYTES);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_IO_FILE_BUFFER_SIZE);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_INDEX_CACHE_MEMORY_LIMIT_BYTES);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_BUFFER_SIZE_MB);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_OUTPUT_MAX_PER_BUFFER_SIZE_BYTES);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PARTITIONER_CLASS);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_KEY_CLASS);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_VALUE_CLASS);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_COMPRESS_CODEC);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_CONVERT_USER_PAYLOAD_TO_HISTORY_TEXT);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_PIPELINED_SHUFFLE_ENABLED);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_ENABLE_FINAL_MERGE_IN_OUTPUT);
            confKeys.add(TezConfiguration.TEZ_COUNTERS_MAX);
            confKeys.add(TezConfiguration.TEZ_COUNTERS_GROUP_NAME_MAX_LENGTH);
            confKeys.add(TezConfiguration.TEZ_COUNTERS_COUNTER_NAME_MAX_LENGTH);
            confKeys.add(TezConfiguration.TEZ_COUNTERS_MAX_GROUPS);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_CLEANUP_FILES_ON_INTERRUPT);
            confKeys.add(TezRuntimeConfiguration.TEZ_RUNTIME_REPORT_PARTITION_STATS);
            confKeys.add(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID);
            confKeys.add(
                    TezRuntimeConfiguration.TEZ_RUNTIME_UNORDERED_PARTITIONED_KVWRITER_BUFFER_MERGE_PERCENT);

            confKeys.add(TEZ_RSS_RUNTIME_SHUFFLE_RSS_ENABLED);
            confKeys.add(TEZ_RSS_RUNTIME_IO_SORT_MB);
            confKeys.add(TEZ_RSS_CLIENT_SORT_MEMORY_USE_THREAD);
            confKeys.add(TEZ_RSS_CLIENT_MAX_BUFFER_SIZE);
            confKeys.add(TEZ_RSS_WRITER_BUFFER_SIZE);
            confKeys.add(TEZ_RSS_CLIENT_MEMORY_THREAD);
            confKeys.add(TEZ_RSS_CLIENT_SEND_THREAD);
            confKeys.add(TEZ_RSS_CLIENT_BATCH_TRIGGER_NUM);
            confKeys.add(TEZ_RSS_STORAGE_TYPE);
            confKeys.add(TEZ_RSS_CLIENT_SEND_CHECK_INTERVAL_MS);
            confKeys.add(TEZ_RSS_CLIENT_SEND_CHECK_TIMEOUT_MS);
            confKeys.add(TEZ_RSS_CLIENT_BITMAP_NUM);
            confKeys.add(HIVE_TEZ_LOG_LEVEL);
        }

        public static Set<String> getConfigurationKeySet() {
            return Collections.unmodifiableSet(confKeys);
        }
    }

    public static class RssKVInput {
        private static final Set<String> confKeys = new HashSet<String>();

        static {
            confKeys.add(TEZ_RSS_RUNTIME_SHUFFLE_RSS_ENABLED);
        }

        // TODO Maybe add helper methods to extract keys
        // TODO Maybe add constants or an Enum to access the keys
        @InterfaceAudience.Private
        public static Set<String> getConfigurationKeySet() {
            return Collections.unmodifiableSet(confKeys);
        }
    }


    public static class RssUnOrderedPartitionedKVInput extends RssKVInput {
        static final String inputClassName = "org.apache.tez.runtime.library.input.RssUnorderedKVInput";

        public static String getInputClassName(){
            LOG.info("Use RssUnOrderedPartitionedKVInput, inputClassName:{}", inputClassName);
            return inputClassName;
        }
    }


    public static class RssOrderedPartitionedKVInput extends RssKVInput {
        static final String inputClassName = "org.apache.tez.runtime.library.input.RssOrderedGroupedKVInput";

        public static String getInputClassName(){
            LOG.info("Use RssOrderedPartitionedKVInput, inputClassName:{}", inputClassName);
            return inputClassName;
        }
    }
}