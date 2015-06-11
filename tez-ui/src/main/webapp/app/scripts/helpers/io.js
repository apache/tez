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

zip.workerScriptsPath = "scripts/zip.js/";

App.Helpers.io = {
  /* Allow queuing of downloads and then get a callback once all the downloads are done.
   * sample usage.
   * var downloader = App.Helpers.io.fileDownloader();
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
        deferredPromise = Em.RSVP.defer();

    function checkForCompletion() {
      if (hasFailed) {
        if (inProgress == 0) {
          deferredPromise.reject("FOOBAR");
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
      $.each(pendingRequests, function(idx, val) {
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
        Em.Logger.debug("delaying download as %@ of %@ is in progress".fmt(inProgress, numParallel));
        return;
      }

      if (itemList.length < 1) {
        Em.Logger.debug("no items to download");
        checkForCompletion();
        return;
      }

      inProgress++;
      Em.Logger.debug("starting download %@".fmt(inProgress));
      var item = itemList.shift();

      var xhr = $.ajax({
        crossOrigin: true,
        url: item.url,
        xhrFields: {
          withCredentials: true
        },
      });
      var reqID = getRequestId();
      pendingRequests[reqID] = xhr;

      xhr.done(function(data, statusText, xhr) {
        delete pendingRequests[reqID];

        if ($.isFunction(item.onItemFetched)) {
          try {
            item.onItemFetched(data, item.context);
          } catch (e) {
            markFailed("invalid data");
            inProgress--;
            checkForCompletion();
            return;
          }
        }

        inProgress--;
        processNext();
      }).fail(function(xhr, statusText, errorObject) {
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
   * zipHelper = App.Helpers.io.zipHelper({
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
        zipFileEntry,
        zipWriter,
        completion = Em.RSVP.defer(),
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
          Em.Logger.debug("aborting zipping. closing file.");
          zipWriter.close(completion.reject);
          zipWriter = null;
        }
      } else {
        if (!hasMoreInputs && numFiles == completed) {
          Em.Logger.debug("completed zipping. closing file.");
          zipWriter.close(completion.resolve);
        }
      }
    }

    function onProgress(current, total) {
      if ($.isFunction(opts.onProgress)) {
        opts.onProgress(fileList[currentIdx].name, current, total);
      }
    }

    function onAdd(filename) {
      if ($.isFunction(opts.onAdd)) {
        opts.onAdd(filename);
      }
    }

    function nextFile() {
      if (hasFailed || completed == numFiles || inProgress) {
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
          Em.Logger.debug("Skipping add of file %@ as zip has been aborted".fmt(file.name));
          return;
        }
        numFiles++;
        fileList.push(file);
        if (zipWriter) {
          Em.Logger.debug("addinng file from addFile: " + file.name);
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
