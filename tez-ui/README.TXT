Dev setup instructions
----------------------
install the development dependencies nodejs, npm, grunt and bower
from the top level directory run the following commands
  * npm install
  * bower install

The timeline server (ATS) url by default points to http://localhost:8188. This can be
changed to point to another host by editing the App.AtsBaseUrl entry in the
app/scripts/app.js file.

For development run 'grunt serve'. This runs a dev server on port 9001. 
navigate to http://localhost:9001 if a browser does not open automatically.
Any changes made will be live-reloaded on the browser.

For creating a distribution version, run 'grunt build'. The distributable 
version will be placed under the dist directory.
