# elasticsearch_to_sql
Dynamic Process to pull elasticsearch data, paginate through results, and post to SQL db

This project is configured to run a single template .py file with a config .yml file to parse elasticsearch results and post the results to SQL. For each new query, a config file can be created and passed in the arguments, so the .py file should never need updates after first setup.

For the query configuration, I use a simple table:

```
CREATE TABLE `elastic_configuration` (
  `date` datetime,
  `interv` int(11) ,
  `period` varchar(15),
  `event` varchar(250) COLLATE utf8mb4_unicode_ci
)```

Sample SQL config output:
![Config Table](https://raw.githubusercontent.com/trentreese/elasticsearch_to_sql/master/config_table.jpg)
