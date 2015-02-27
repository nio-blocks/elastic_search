ESBase
===========

Stores input signals in a elasticsearch database. One document will be inserted into the database for each input signal.

Properties
--------------

-   **index**: The name of the index (equivalent to database)
-   **type**: The type of the document (equivalent to table)

Dependencies
----------------
-   [elasticsearch](https://pypi.python.org/pypi/elasticsearch/1.4.0)

Commands
----------------
-   connected: determines if elasticsearch server is available
-   search: performs a search from given 'body' parameters

Input
-------
One document will be inserted into the database for each input signal.

Output
---------
None
