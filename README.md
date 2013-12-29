#RETS HELPER

PHP Class to interface RETS with Database. This software requires the use of PHRETS.

The source code for PHRETS is available on [GitHub](http://github.com/troydavisson/PHRETS)


## Introduction

Download and drop the project files into a directory in your localhost or web server.

In ```rets_helper.php```:
* Define the ```BASE_PATH```
* Set your database info in ```___construct()```
* Set ```$this->config['create_tables']``` flag to ```TRUE``` and run.

Two tables for the COM and RES classes will be created. Go back and change the flag to ```FALSE``` and run again. Listing data will be inserted into the tables and download photos for listings.
