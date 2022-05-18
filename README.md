# Data Structures Control

_Data Structures Control_, or `dsctl` for short, is a client to the 
Snowplow BDP Data Structures API. This API can be used by Snowplow
Analytics customers as elaborated in 
[the respective documentation](https://docs.snowplowanalytics.com/docs/understanding-tracking-design/managing-data-structures-via-the-api-2/).
To invoke the API, one needs to [authenticate with a JSON web token](https://docs.snowplowanalytics.com/docs/using-the-snowplow-console/managing-console-api-authentication/).

The `dsctl` script has been built with CI/CD
pipelines in mind, for example a git-flow like workflow where schemas
merged into `develop` end up to a development/QA pipeline while schemas
merged into the main branch are deployed in production.

The first iteration of the script includes the following functionality:
1. Authenticating with the API
2. Validating a data structure
3. Promoting a validated data structure to dev
4. Promoting a data structure from dev to prod.

The `samples` directory contains two sample inputs and a script that
uses them. In general, input to the script can come from stdin or read
from a file using the `--file` parameter. All the available parameters
are as follows:

```
  -h, --help            show this help message and exit
  --token-only          only get an access token and print it on stdout
  --token TOKEN         use this token to authenticate
  --file FILE           read schema from file (absolute path) instead of stdin
  --type {event,entity}
                        document type
  --includes-meta       the input document already contains the meta field
  --promote-to-dev      promote from validated to dev; reads parameters from stdin or schema file
  --promote-to-prod     promote from dev to prod; reads parameters from stdin or schema file
  --message MESSAGE     message to add to version deployment
```

By default, when given no arguments, the script will validate its input.

## Environment variables

The following non-optional environment variables must be set:

- `CONSOLE_ORGANIZATION_ID` -- the organization ID as it can be found in BDP Console
- `CONSOLE_API_KEY` -- the API key generated via the [BDP Console UI](https://console.snowplowanalytics.com/credentials)
