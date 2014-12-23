sortdb
======

Sortdb makes a sorted tab (tsv) or comma (csv) delimitated sorted file accessible via HTTP.

[![Build Status](https://secure.travis-ci.org/jehiah/sortdb.svg?branch=master)](http://travis-ci.org/jehiah/sortdb)


    Usage of ./sortdb:
      -db-file="": db file
      -field-separator="\t": field separator (eg: comma, tab, pipe)
      -http-address=":8080": http address to listen on

API endpoints:

 * `/ping`  responsds with 200 `OK`

 * `/get?key=...`
    
 * `/mget?k=&k=...` *(not implemented yet)*

 * `/stats` *(not implemented yet)*
 
 * `/reload` reload/remap the db file
 
 * `/exit` cause the current process to exit

a HUP signal will also cause sortdb to reload/remap the db file

--

Sortdb was originally developed by [@jayridge](https://twitter.com/jayridge) as part of the [simplehttp project](https://github.com/bitly/simplehttp/tree/master/sortdb) and was ported to Go by [Jehiah Czebotar](https://jehiah.cz/)