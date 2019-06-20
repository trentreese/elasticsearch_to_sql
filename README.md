# elasticsearch_to_sql
<h2>Dynamic Process to pull elasticsearch data, paginate through results, and post to SQL db</h2>

<h4>This project is configured to run a single template .py file with a config .yml file to parse elasticsearch results and post the results to SQL. For each new query, a config file can be created and passed in the arguments, so the .py file should never need updates after first setup.</h4>

For the query configuration, I use a simple table:

```
CREATE TABLE `elastic_configuration` (
  `date` datetime,
  `interv` int(11) ,
  `period` varchar(15),
  `event` varchar(250) COLLATE utf8mb4_unicode_ci)
```

Sample SQL config output: 
<br>
![Config Table](config_table.jpg)

In the .yml file, define the fields in your destination table:
  fields:
        - datetime # for the timestamp field, included by default.
        - country # sample fields, add or remove as desired. Must match the fields retrieved from ES.
        - user_id
        - uuid
        - action
        - context
        - referrer
        
Then define the path to these fields in the elasticsearch JSON results:
  fields: # sample path to field values. Code looks to nested hits / hits for attributes.
         - country: '_source.query_params.country'
         - user_id: '_source.query_params.user_id'
         - uuid: '_source.query_params.uuid'
         - action: '_source.query_params.action'
         - context: '_source.query_params.context'
         - referrer: '_source.referrer'

Requirements:
DateTime>=4.3
mysql-connector>=2.1.4
pandas>=0.23.4
PyYAML>=3.13
SQLAlchemy>=1.2.14
timedelta>=2018.11.20
