Data Detectives
Introduction to Big Data Fall 2015
Ted Sill, Kyle Brennan, and Sawan-Kumar Jindal

We retrieved a large dataset (39,797 records) from the UCI Machine Learning Repository 
(http://archive.ics.uci.edu/ml/datasets/Online+News+Popularity).  Each record in this file 
contains a large number of attributes (61).  In order to make the data easier to work with 
we designed a relational schema.  This console application was created to map the flat structure 
of the csv file (OnlineNewsPopularity.csv) into our database.

The Program.cs file contains code which controls the overall flow.  It dispatches units of work, 
the bulk of which is done in MySQLRepository.  This manages the connections to the database, and
executes the queries which populate it.

Before you execute this application you should have the 'news' schema in place in a local MySQL instance.
This can be restored from the 'news_backup_empty.sql' file included with this submission.
The 'root' user will need access to query and insert records.  You may change the connection string in
the App.Config file if you wish to use a different user to connect to the database.  Depending on
your hardware executing this application may take around 13 hours to complete.  Alternatively,
you can restore the fully populated database from the 'news_backup.sql' also included with this submission.