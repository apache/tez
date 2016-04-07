/*global zip, saveAs*/
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import Ember from 'ember';
import DS from 'ember-data';

zip.workerScriptsPath = "assets/zip/";

var IO = {
  /* Allow queuing of downloads and then get a callback once all the downloads are done.
   * sample usage.
   * var downloader = IO.fileDownloader();
   * downloader.queueItem({
   *   url: 'http://....',
   *   onItemFetched: function(data, context) {...},
   *   context: {}, // context object gets passed back to the callback
   * });
   * downloader.queueItem({...}); //queue in other items
   * downloader.finish(); // once all items are queued. items can be queued from
   *                      // callbacks too. in that case the finish should be called
   *                      // once all items are queued.
   * downloader.then(successCallback).catch(failurecallback).finally(callback)
   */
  fileDownloader: function(options) {
    var itemList = [],
        opts = options || {},
        numParallel = opts.numParallel || 5,
        hasMoreInputs = true,
        inProgress = 0,
        hasFailed = false,
        pendingRequests = {},
        pendingRequestID = 0,
        failureReason = 'Unknown',
        deferredPromise = Ember.RSVP.defer();

    function checkForCompletion() {
      if (hasFailed) {
        if (inProgress === 0) {
          deferredPromise.reject("Unknown Error");
        }
        return;
      }

      if (hasMoreInputs || itemList.length > 0 || inProgress > 0) {
        return;
      }

      deferredPromise.resolve();
    }

    function getRequestId() {
      return "req_" + pendingRequestID++;
    }

    function abortPendingRequests() {
      Ember.$.each(pendingRequests, function(idx, val) {
        try {
          val.abort("abort");
        } catch(e) {}
      });
    }

    function markFailed(reason) {
      if (!hasFailed) {
        hasFailed = true;
        failureReason = reason;
        abortPendingRequests();
      }
    }

    function processNext() {
      if (inProgress >= numParallel) {
        Ember.Logger.debug(`delaying download as ${inProgress} of ${numParallel} is in progress`);
        return;
      }

      if (itemList.length < 1) {
        Ember.Logger.debug("no items to download");
        checkForCompletion();
        return;
      }

      inProgress++;
      Ember.Logger.debug(`starting download ${inProgress}`);
      var item = itemList.shift();

      var xhr = Ember.$.ajax({
        crossOrigin: true,
        url: item.url,
        dataType: 'json',
        xhrFields: {
          withCredentials: true
        },
      });
      var reqID = getRequestId();
      pendingRequests[reqID] = xhr;

      xhr.done(function(data/*, statusText, xhr*/) {
        delete pendingRequests[reqID];

        if (Ember.$.isFunction(item.onItemFetched)) {
          try {
            item.onItemFetched(data, item.context);
          } catch (e) {
            markFailed(e || 'failed to process data');
            inProgress--;
            checkForCompletion();
            return;
          }
        }

        inProgress--;
        processNext();
      }).fail(function(xhr, statusText/*, errorObject*/) {
        delete pendingRequests[reqID];
        markFailed(statusText);
        inProgress--;
        checkForCompletion();
      });
    }

    return DS.PromiseObject.create({
      promise: deferredPromise.promise,

      queueItems: function(options) {
        options.forEach(this.queueItem);
      },

      queueItem: function(option) {
        itemList.push(option);
        processNext();
      },

      finish: function() {
        hasMoreInputs = false;
        checkForCompletion();
      },

      cancel: function() {
        markFailed("User cancelled");
        checkForCompletion();
      }
    });
  },


  /*
   * allows to zip files and download that.
   * usage:
   * zipHelper = IO.zipHelper({
   *   onProgress: function(filename, current, total) { ...},
   *   onAdd: function(filename) {...}
   * });
   * zipHelper.addFile({name: filenameinsidezip, data: data);
   * // add all files
   * once all files are added call the close
   * zipHelper.close(); // or .abort to abort zip
   * zipHelper.then(function(zippedBlob) {
   *   saveAs(filename, zippedBlob);
   * }).catch(failureCallback);
   */
  zipHelper: function(options) {
    var opts = options || {},
        zipWriter,
        completion = Ember.RSVP.defer(),
        fileList = [],
        completed = 0,
        currentIdx = -1,
        numFiles = 0,
        hasMoreInputs = true,
        inProgress = false,
        hasFailed = false;

    zip.createWriter(new zip.BlobWriter("application/zip"), function(writer) {
      zipWriter = writer;
      checkForCompletion();
      nextFile();
    });

    function checkForCompletion() {
      if (hasFailed) {
        if (zipWriter) {
          Ember.Logger.debug("aborting zipping. closing file.");
          zipWriter.close(completion.reject);
          zipWriter = null;
        }
      } else {
        if (!hasMoreInputs && numFiles === completed) {
          Ember.Logger.debug("completed zipping. closing file.");
          zipWriter.close(completion.resolve);
        }
      }
    }

    function onProgress(current, total) {
      if (Ember.$.isFunction(opts.onProgress)) {
        opts.onProgress(fileList[currentIdx].name, current, total);
      }
    }

    function onAdd(filename) {
      if (Ember.$.isFunction(opts.onAdd)) {
        opts.onAdd(filename);
      }
    }

    function nextFile() {
      if (hasFailed || completed === numFiles || inProgress) {
        return;
      }

      currentIdx++;
      var file = fileList[currentIdx];
      inProgress = true;
      onAdd(file.name);
      zipWriter.add(file.name, new zip.TextReader(file.data), function() {
        completed++;
        inProgress = false;
        if (currentIdx < numFiles - 1) {
          nextFile();
        }
        checkForCompletion();
      }, onProgress);
    }

    return DS.PromiseObject.create({
      addFiles: function(files) {
        files.forEach(this.addFile);
      },

      addFile: function(file) {
        if (hasFailed) {
          Ember.Logger.debug(`Skipping add of file ${file.name} as zip has been aborted`);
          return;
        }
        numFiles++;
        fileList.push(file);
        if (zipWriter) {
          Ember.Logger.debug("adding file from addFile: " + file.name);
          nextFile();
        }
      },

      close: function() {
        hasMoreInputs = false;
        checkForCompletion();
      },

      promise: completion.promise,

      abort: function() {
        hasFailed = true;
        this.close();
      }
    });
  }
};

export default function downloadDagZip(dag, options) {
  var opts = options || {},
      batchSize = opts.batchSize || 1000,
      dagID = dag.get("entityID"),
      baseurl = `${options.timelineHost}/${options.timelineNamespace}`,
      itemsToDownload = [
        {
          url: getUrl('TEZ_APPLICATION', 'tez_' + dag.get("appID")),
          context: { name: 'application', type: 'TEZ_APPLICATION' },
          onItemFetched: processSingleItem
        },
        {
          url: getUrl('TEZ_DAG_ID', dagID),
          context: { name: 'dag', type: 'TEZ_DAG_ID' },
          onItemFetched: processSingleItem
        },
        {
          url: getUrl('TEZ_VERTEX_ID', dagID),
          context: { name: 'vertices', type: 'TEZ_VERTEX_ID', part: 0 },
          onItemFetched: processMultipleItems
        },
        {
          url: getUrl('TEZ_TASK_ID', dagID),
          context: { name: 'tasks', type: 'TEZ_TASK_ID', part: 0 },
          onItemFetched: processMultipleItems
        },
        {
          url: getUrl('TEZ_TASK_ATTEMPT_ID', dagID),
          context: { name: 'task_attempts', type: 'TEZ_TASK_ATTEMPT_ID', part: 0 },
          onItemFetched: processMultipleItems
        }
      ],
      totalItemsToDownload = itemsToDownload.length,
      numItemTypesToDownload = totalItemsToDownload,
      downloader = IO.fileDownloader(),
      zipHelper = IO.zipHelper({
        onProgress: function(filename, current, total) {
          Ember.Logger.debug(`${filename}: ${current} of ${total}`);
        },
        onAdd: function(filename) {
          Ember.Logger.debug(`adding ${filename} to Zip`);
        }
      }),
      downloaderProxy = Ember.Object.create({
        percent: 0,
        succeeded: false,
        failed: false,
        cancel: function() {
          downloader.cancel();
        }
      });

  function getUrl(type, dagID, fromID) {
    var url,
        queryBatchSize = batchSize + 1;

    if (type === 'TEZ_DAG_ID' || type === 'TEZ_APPLICATION') {
      url = `${baseurl}/${type}/${dagID}`;
    } else {
      url = `${baseurl}/${type}?primaryFilter=TEZ_DAG_ID:${dagID}&limit=${queryBatchSize}`;
      if (!!fromID) {
        url = `${url}&fromId=${fromID}`;
      }
    }
    return url;
  }

  function checkIfAllDownloaded() {
    numItemTypesToDownload--;

    var remainingItems = totalItemsToDownload - numItemTypesToDownload;
    downloaderProxy.set("percent", remainingItems / totalItemsToDownload);

    if (numItemTypesToDownload === 0) {
      downloader.finish();
    }
  }

  function processSingleItem(data, context) {
    var obj = {};
    obj[context.name] = data;

    zipHelper.addFile({name: `${context.name}.json`, data: JSON.stringify(obj, null, 2)});
    checkIfAllDownloaded();
  }

  function processMultipleItems(data, context) {
    var obj = {};
    var nextBatchStart;

    if (!Ember.$.isArray(data.entities)) {
      throw "invalid data";
    }

    // need to handle no more entries , zero entries
    if (data.entities.length > batchSize) {
      nextBatchStart = data.entities.pop().entity;
    }
    obj[context.name] = data.entities;

    zipHelper.addFile({name: `${context.name}_part_${context.part}.json`, data: JSON.stringify(obj, null, 2)});

    if (!!nextBatchStart) {
      context.part++;
      downloader.queueItem({
        url: getUrl(context.type, dagID, nextBatchStart),
        context: context,
        onItemFetched: processMultipleItems
      });
    } else {
      checkIfAllDownloaded();
    }
  }

  downloader.queueItems(itemsToDownload);

  downloader.then(function() {
    Ember.Logger.info('Finished download');
    zipHelper.close();
  }).catch(function(e) {
    Ember.Logger.error('Failed to download: ' + e);
    zipHelper.abort();
  });

  zipHelper.then(function(zippedBlob) {
    saveAs(zippedBlob, `${dagID}.zip`);
    downloaderProxy.set("succeeded", true);
  }, function() {
    Ember.Logger.error('zip Failed');
    downloaderProxy.set("failed", true);
  });

  return downloaderProxy;
}
