# apidGatewayConfDeploy

[![Build Status](https://travis-ci.org/apid/apidGatewayConfDeploy.svg)](https://travis-ci.org/apid/apidGatewayConfDeploy) [![GoDoc](https://godoc.org/github.com/apid/apidGatewayConfDeploy?status.svg)](https://godoc.org/github.com/apid/apidGatewayConfDeploy) [![Go Report Card](https://goreportcard.com/badge/github.com/apid/apidGatewayConfDeploy)](https://goreportcard.com/report/github.com/apid/apidGatewayConfDeploy)

apidGatewayConfDeploy is a plugin for
[apid](http://github.com/apid/apid).

Gateways acting as clients will connect to apid; and this plugin will
offer configuration updates that occur in the management. The plugin
will also download the configuration data from GCS securely, and store
it in the local filesystem. They can be fetched by the Gateway via API's
as well.

## Functional description

###Configurations
* Gateway cant call "/configurations" to fetch configurations.
* "type" filter is supported.
* Long-polling is supported.
* A configuration can be fetched by id "/configurations/{configId}"

###Blobs
* A blob can be downloaded by id "/blobs/{blobId}"


For details, check the file [apidGatewayConfDeploy-api.yaml](swagger.yaml).

