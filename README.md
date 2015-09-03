ESBase
===========

Provides base class for elasticsearch blocks.

Each block defines a query to be executed. If that query fails then one retry will be attempted after first waiting one second. Only if this retry fails will the block log an error.

Properties
--------------

-   **index**: The name of the index (equivalent to database)
-   **doc_type**: The type of the document (equivalent to table)

Dependencies
----------------
-   [elasticsearch](https://pypi.python.org/pypi/elasticsearch/1.4.0)

Commands
----------------
connected: determines if elasticsearch server is available
search: performs a search for given 'body' and 'params' parameters

Input
-------
None

Output
---------
None

----------------

ESInsert
===========

Stores input signals in a elasticsearch database. One document will be inserted into the database for each input signal.

Properties
--------------

-   **with_type**: If True, includes the signal type in the document.

Dependencies
----------------
-   [elasticsearch](https://pypi.python.org/pypi/elasticsearch/1.4.0)

Commands
----------------
None

Input
-------
One document will be inserted into the database for each input signal.

Output
---------
A signal with "id" field obtained from inserted document


----------------

ESFind
===========

Finds elements from given search parameters

Properties
--------------

-   **condition**: Condition to filter data on
-   **size**: Number of elements to return (empty string to not include in query)
-   **offset**: Starting offset to use when returning data (empty string to not include in query)
-   **sort**: Sorting parameters if any

Dependencies
--------------
-   [elasticsearch](https://pypi.python.org/pypi/elasticsearch/1.4.0)

Commands
--------------
None

Input
-------
Signals to be processed

Output
---------
Data satisfying find criteria as 'Signal' instances.
