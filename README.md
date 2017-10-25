# apidGatewayConfDeploy

apidGatewayConfDeploy is a plugin for
[apid](http://github.com/30x/apid).

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

