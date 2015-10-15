curl -XPUT 'http://localhost:9200/ngas' -d '
{
  "mappings" : {
    "access" : {
      "properties" : {
        "date" : {
          "type" : "date",
          "format" : "date_hour_minute_second_millis"
        },
        "ip" : {
          "type" : "ip"
        },
        "obsDate" : {
          "type" : "date",
          "format" : "yyyyMMDDHHmmss"
        },
        "host": {
          "type": "string",
          "index": "not_analyzed"
        },
        "obsId": {
           "type": "long"
        },
        "size": {
           "type": "double"
        },
        "file": {
          "type": "string",
          "index": "not_analyzed"
        }
      }
    },
    "ingest" : {
      "properties" : {
        "date" : {
          "type" : "date",
          "format" : "date_hour_minute_second_millis"
        },
        "ip" : {
          "type" : "ip"
        },
        "obsDate" : {
          "type" : "date",
          "format" : "yyyyMMDDHHmmss"
        },
        "host": {
          "type": "string",
          "index": "not_analyzed"
        },
        "obsId": {
           "type": "long"
        },
        "size": {
           "type": "double"
        },
        "file": {
          "type": "string",
          "index": "not_analyzed"
        }
      }
    }
  }
}
' 
