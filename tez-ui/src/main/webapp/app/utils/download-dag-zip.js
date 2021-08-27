/*global zip */
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

import EmberObject from '@ember/object';
import { defer } from 'rsvp';
import { later } from '@ember/runloop';
import { saveAs } from 'file-saver';

import MoreObject from '../utils/more-object';

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
        deferredPromise = defer();

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
      Object.values(pendingRequests).forEach(function (pendingRequest) {
        try {
          pendingRequest.abort("abort");
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
        console.debug(`delaying download as ${inProgress} of ${numParallel} is in progress`);
        return;
      }

      if (itemList.length < 1) {
        console.warn("no items to download");
        checkForCompletion();
        return;
      }

      inProgress++;
      console.warn(`starting download ${inProgress}`);
      var item = itemList.shift();

      var xhr = new XMLHttpRequest();
      xhr.open('GET', item.url, true);
      xhr.withCredentials = true;
      xhr.responseType='json';

      var reqID = getRequestId();
      pendingRequests[reqID] = xhr;

      if(item.onFetch) {
        item.onFetch(item.context);
      }

      xhr.onload = function() {
        delete pendingRequests[reqID];

        if (xhr.status !== 200) {
          // handle valid responses like 404
          if(item.retryCount) {
            itemList.unshift(item);
            item.retryCount--;
            if(item.onRetry) {
              item.onRetry(item.context);
            }
            later(processNext, 3000 + Math.random() * 3000);
          }
          else if(item.onItemFail) {
            item.onItemFail(xhr, item.context);
            processNext();
          }
          else {
            markFailed(xhr.statusText || 'failed to fetch data');
            checkForCompletion();
            return;
          }
        }
        if (MoreObject.isFunction(item.onItemFetched)) {
          try {
            item.onItemFetched(xhr.response, item.context);
          } catch (e) {
            markFailed(e || 'failed to process data');
            inProgress--;
            checkForCompletion();
            return;
          }
        }

        inProgress--;
        processNext();
      };
      xhr.onerror = function() {
        delete pendingRequests[reqID];
        inProgress--;

        if(item.retryCount) {
          itemList.unshift(item);
          item.retryCount--;
          if(item.onRetry) {
            item.onRetry(item.context);
          }
          later(processNext, 3000 + Math.random() * 3000);
        }
        else if(item.onItemFail) {
          item.onItemFail(xhr, item.context);
          processNext();
        }
        else {
          markFailed(e);
          checkForCompletion();
        }
      };
      xhr.send();
    }

      deferredPromise.queueItems = function(options) {
        options.forEach(this.queueItem);
      },

      deferredPromise.queueItem = function(option) {
        itemList.push(option);
        processNext();
      },

      deferredPromise.finish = function() {
        hasMoreInputs = false;
        checkForCompletion();
      },

      deferredPromise.cancel = function() {
        markFailed("User cancelled");
        checkForCompletion();
      }
    return deferredPromise;
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
        completion = defer(),
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
          console.warn("aborting zipping. closing file.");
          zipWriter.close(completion.reject);
          zipWriter = null;
        }
      } else {
        if (!hasMoreInputs && numFiles === completed) {
          console.warn("completed zipping. closing file.");
          zipWriter.close(completion.resolve);
        }
      }
    }

    function onProgress(current, total) {
      if (MoreObject.isFunction(opts.onProgress)) {
        opts.onProgress(fileList[currentIdx].name, current, total);
      }
    }

    function onAdd(filename) {
      if (MoreObject.isFunction(opts.onAdd)) {
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

      completion.addFiles = function(files) {
        files.forEach(this.addFile);
      };

      completion.addFile = function(file) {
        if (hasFailed) {
          console.warn(`Skipping add of file ${file.name} as zip has been aborted`);
          return;
        }
        numFiles++;
        fileList.push(file);
        if (zipWriter) {
          console.warn("adding file from addFile: " + file.name);
          nextFile();
        }
      };

      completion.close = function() {
        hasMoreInputs = false;
        checkForCompletion();
      };

      completion.abort = function() {
        hasFailed = true;
        this.close();
      };
    return completion;
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
          onFetch: onFetch,
          onRetry: onRetry,
          onItemFetched: processSingleItem,
          onItemFail: processFailure,
          retryCount: 3,
        },
        {
          url: getUrl('TEZ_DAG_EXTRA_INFO', dagID),
          context: { name: 'dag-extra-info', type: 'TEZ_DAG_EXTRA_INFO' },
          onFetch: onFetch,
          onRetry: onRetry,
          onItemFetched: processSingleItem,
          onItemFail: processFailure,
          retryCount: 3,
        },
        {
          url: getUrl('TEZ_DAG_ID', dagID),
          context: { name: 'dag', type: 'TEZ_DAG_ID' },
          onFetch: onFetch,
          onRetry: onRetry,
          onItemFetched: processSingleItem,
          onItemFail: processFailure,
          retryCount: 3,
        },
        {
          url: getUrl('TEZ_VERTEX_ID', null, dagID),
          context: { name: 'vertices', type: 'TEZ_VERTEX_ID', part: 0 },
          onFetch: onFetch,
          onRetry: onRetry,
          onItemFetched: processMultipleItems,
          onItemFail: processFailure,
          retryCount: 3,
        },
        {
          url: getUrl('TEZ_TASK_ID', null, dagID),
          context: { name: 'tasks', type: 'TEZ_TASK_ID', part: 0 },
          onFetch: onFetch,
          onRetry: onRetry,
          onItemFetched: processMultipleItems,
          onItemFail: processFailure,
          retryCount: 3,
        },
        {
          url: getUrl('TEZ_TASK_ATTEMPT_ID', null, dagID),
          context: { name: 'task_attempts', type: 'TEZ_TASK_ATTEMPT_ID', part: 0 },
          onFetch: onFetch,
          onRetry: onRetry,
          onItemFetched: processMultipleItems,
          onItemFail: processFailure,
          retryCount: 3,
        }
      ];

      let callerID = options.callerInfo.id,
          entityType = options.callerInfo.type;
      if(callerID && entityType) {
        itemsToDownload.push({
          url: getUrl(entityType, callerID),
          context: { name: entityType.toLocaleLowerCase(), type: entityType },
          onItemFetched: processSingleItem,
          onItemFail: checkIfAllDownloaded
        });
      }

  var totalItemsToDownload = itemsToDownload.length,
      numItemTypesToDownload = totalItemsToDownload,
      downloader = IO.fileDownloader(),
      zipHelper = IO.zipHelper({
        onProgress: function(filename, current, total) {
        },
        onAdd: function(filename) {
          console.warn(`adding ${filename} to Zip`);
        }
      }),
      downloaderProxy = EmberObject.create({
        percent: 0,
        message: "",
        succeeded: false,
        partial: false,
        failed: false,
        cancel: function() {
          downloader.cancel();
          zipHelper.abort();
        }
      });

  function getUrl(type, id, dagID, fromID) {
    var url,
        queryBatchSize = batchSize + 1;

    if (id) {
      url = `${baseurl}/${type}/${id}`;
    } else {
      url = `${baseurl}/${type}?primaryFilter=TEZ_DAG_ID:"${dagID}"&limit=${queryBatchSize}`;
      if (fromID) {
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

  function onFetch(context) {
    downloaderProxy.set("message", `Fetching ${context.name} data.`);
  }

  function onRetry(context) {
    downloaderProxy.set("message", `Downloading ${context.name} data failed. Retrying!`);
  }

  function processFailure(data, context) {
    var obj = {};
    try {
      obj[context.name] = JSON.parse(data.responseText);
    }
    catch(e) {
      obj[context.name] = data.responseText;
    }

    downloaderProxy.set("partial", true);
    downloaderProxy.set("message", `Downloading ${context.name} data failed!`);

    zipHelper.addFile({name: `error.${context.name}.json`, data: JSON.stringify(obj, null, 2)});
    checkIfAllDownloaded();
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

    if (!Array.isArray(data.entities)) {
      throw "invalid data";
    }

    // need to handle no more entries , zero entries
    if (data.entities.length > batchSize) {
      nextBatchStart = data.entities.pop().entity;
    }
    obj[context.name] = data.entities;

    zipHelper.addFile({name: `${context.name}_part_${context.part}.json`, data: JSON.stringify(obj, null, 2)});

    if (nextBatchStart) {
      context.part++;
      downloader.queueItem({
        url: getUrl(context.type, null, dagID, nextBatchStart),
        context: context,
        onItemFetched: processMultipleItems
      });
    } else {
      checkIfAllDownloaded();
    }
  }

  downloader.queueItems(itemsToDownload);

  downloader.promise.then(function() {
    console.info('Finished download');
    zipHelper.close();
  }).catch(function(e) {
    console.error('Failed to download: ' + e);
    zipHelper.abort();
  });

  zipHelper.promise.then(function(zippedBlob) {
    saveAs(zippedBlob, `${dagID}.zip`);
    downloaderProxy.set("message", `Download complete.`);
    downloaderProxy.set("succeeded", true);
  }, function() {
    console.error('zip Failed');
    downloaderProxy.set("failed", true);
  });

  return downloaderProxy;
}
