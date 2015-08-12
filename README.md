sortdb
======

Sortdb makes a sorted tab (tsv) or comma (csv) delimitated file accessible via HTTP.

[![Build Status](https://secure.travis-ci.org/jehiah/sortdb.svg?branch=master)](http://travis-ci.org/jehiah/sortdb)


    Usage of ./sortdb:
      -db-file="": db file
      -enable-logging=false: request logging
      -field-separator="\t": field separator (eg: comma, tab, pipe)
      -http-address=":8080": http address to listen on
      -version=false: print version string

Records are matched by the first data column.

### API endpoints:

 * `/ping`  responsds with 200 `OK`

 * `/get?key=...` Response is `text/plain` with the full record that matched
   (excluding the key), or a 404 if no match.
    
 * `/mget?key=...&key=...` Response is `text/plain` with all records that match
   (including the key), or an empty 200 if no matches

 * `/fwmatch?key=...` Response is `text/plain` with the full records that have
   keys lexically greater than or equal to the key, or a 404 if no such records
   exist.

 * `/range?start=...&end=...` Response is `text/plain` with the full records
   that have keys lexically greater than or equal to the start key and less
   than or equal to the end key, or a 404 if no such records exist. The end key
   must be lexically greater than or equal to the start key.

 * `/stats` Response is `application/json` with the following payload

```json
{
  "total_requests": 3,
}
{
  "total_requests":2
  "total_seeks":24
  "get_requests": 3,
  "get_hits": 3,
  "get_misses": 0,
  "get_average_request": 448,
  "get_95": 1323,
  "get_99": 1323,
  "mget_requests": 0,
  "mget_hits": 0,
  "mget_misses": 0,
  "mget_average_request": 0,
  "mget_95": 0,
  "mget_99": 0,
  "fwmatch_requests":1
  "fwmatch_hits":1
  "fwmatch_misses":0
  "fwmatch_average_request":10
  "fwmatch_95":10
  "fwmatch_99":10
  "range_requests":2
  "range_hits":1
  "range_misses":1
  "range_average_request":18
  "range_95":24
  "range_99":24
  "db_size": 767557632,
  "db_mtime": 1435463934
}
```
 
 * `/reload` reload/remap the db file
 
 * `/exit` cause the current process to exit
 
 * `/debug/pprof` the [net/http/pprof](http://golang.org/pkg/net/http/pprof/) debugging endpoints

a HUP signal will also cause sortdb to reload/remap the db file

--

###  Sorting Files

The easiest way to sort an existing datafile is with the unix [sort](http://unixhelp.ed.ac.uk/CGI/man-cgi?sort) utility.

```bash
LC_COLLATE=C sort data.csv > sorted_data.csv
```

Note: The locale specified by the environment affects sort order. Set `LC_ALL=C` or `LC_COLLATE=C` to get the traditional sort order that uses native byte values.

--

Sortdb was originally developed by [@jayridge](https://github.com/jayridge) as part of the [simplehttp project](https://github.com/bitly/simplehttp/tree/master/sortdb) and was ported to Go by [Jehiah Czebotar](https://jehiah.cz/)
