# apidGatewayConfDeploy

apidGatewayConfDeploy is a plugin for
[apid](http://github.com/30x/apid).

Gateways acting as clients will connect to apid; and this plugin will
offer configuration updates that occur in the management. The plugin
will also download the configuration data from GCS securely, and store
it in the local filesystem. They can be fetched by the Gateway via API's
as well.

## Functional description

see the file [swagger.yaml](swagger.yaml).

