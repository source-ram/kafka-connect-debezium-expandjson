# Kafka connect SMT to expand JSON string created by Debezium connector for JSON & JSONB coloumns [WIP] 

THis is Kafka connect SMT (Single Message Transformation) to find all the debezium json/jsonb feilds and expand string value of the feild to JSON object and update the schema. You can also add more feilds to be converted on top of debezium feilds.
This also support mutliple topic

This is work in process and will remove [WIP] when it is prod ready

## Config
Use it in connector config file like this:
~~~json
...
"transforms": "expand",
"transforms.expand.type": "com.source.ram.expandjsondbz.ExpandJSON$Value",
"transforms.expand.sourceFields": "topic1@feild1,topic1@feild2,topic1@feild2,topic1@feild1"
...
~~~



Reused code from https://github.com/RedHatInsights/expandjsonsmt to support debezium schema & mutiple topics

## Build release file
- Remove `target` directory if it exists.
- Increment version in `pom.xml` (e.g. to `0.0.3`).
- Run build script: `./scripts/build_release.sh 0.0.3`.
- Take `*.tar.gz` file from `target` folder and publish it.
