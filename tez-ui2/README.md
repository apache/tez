# Tez-ui

The Tez UI is an ember based web application that provides visualization of Tez applications
running on the Apache Hadoop YARN framework.

For more information on Tez and the Tez UI - the [tez homepage](http://tez.apache.org/ "Apache Tez Homepage").

## Prerequisites

You will need the following things properly installed on your computer.

* [Git](http://git-scm.com/)
* [Node.js](http://nodejs.org/) (with NPM)
* [Ember CLI](http://www.ember-cli.com/)
* [PhantomJS](http://phantomjs.org/)

## Installation

* `git clone <repository-url>` this repository
* In tez/tez-ui2/src/main/webapp
* `npm install`

## Configuring
* By default timeline is expected at localhost:8188 & RM at localhost:8088
* You can point the UI to custom locations by setting the environment variables in src/main/webapp/config/configs.env

## Running / Development

* `ember server`
* Visit your app at [http://localhost:4200](http://localhost:4200).

### Running Tests

* `ember test`
* `ember test --server`

### Building

* `ember build` (development)
* `ember build --environment production` (production)
