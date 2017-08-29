ESFind
======
Finds elements from given search parameters.

Properties
----------
- **auth**: Username and password credentials to connect to the Elastic Search database.
- **condition**: Condition to filter data on.
- **doc_type**: The type of the document to query.
- **elasticsearch_client_kwargs**: kwargs to be passed to elasticsearch client. (e.g.: {'maxsize': 15})
- **enrich**: *enrich_field:* The attribute on the signal to store the results from this block. If this is empty, the results will be merged onto the incoming signal. This is the default operation. Having this field allows a block to 'save' the results of an operation to a single field on an incoming signal and notify the enriched signal.  *results field:* The attribute on the signal to store the results from this block. If this is empty, the results will be merged onto the incoming signal. This is the default operation. Having this field allows a block to save the results of an operation to a single field on an incoming signal and notify the enriched signal.
- **host**: The Elastic Search database's host address.
- **index**: The name of the index.
- **offset**: Starting offset to use when returning data (empty string to not include in query).
- **port**: The port where the Elastic Search database is located
- **pretty_results**: If true, only include query results and no other extraneous information.
- **retry_options**: Configurables for retrying connection.
- **size**: Number of elements to return (empty string to not include in query).
- **sort**: Parameters to sort results by.

Inputs
------
- **default**: Any list of signals.

Outputs
-------
- **default**: Data satisfying find criteria as 'Signal' instances.

Commands
--------
- **connected**: Determines if elasticsearch server is available.

Dependencies
------------
-   [elasticsearch](https://pypi.python.org/pypi/elasticsearch/1.4.0)

ESInsert
========
Stores input signals in a elasticsearch database. One document will be inserted into the database for each input signal.

Properties
----------
- **auth**: Username and password credentials to connect to the Elastic Search database.
- **doc_type**: The type of the document to query.
- **elasticsearch_client_kwargs**: kwargs to be passed to elasticsearch client. (e.g.: {'maxsize': 15})
- **enrich**: *enrich_field:* The attribute on the signal to store the results from this block. If this is empty, the results will be merged onto the incoming signal. This is the default operation. Having this field allows a block to 'save' the results of an operation to a single field on an incoming signal and notify the enriched signal.  *results field:* The attribute on the signal to store the results from this block. If this is empty, the results will be merged onto the incoming signal. This is the default operation. Having this field allows a block to save the results of an operation to a single field on an incoming signal and notify the enriched signal.
- **host**: The Elastic Search database's host address.
- **index**: The name of the index.
- **port**: The port where the Elastic Search database is located
- **retry_options**: Configurables for retrying connection.
- **with_type**: If True, includes the signal type in the document.

Inputs
------
- **default**: Any list of signals.  One document will be inserted into the database for each input signal.

Outputs
-------
- **default**: A signal with 'id' field obtained from inserted document.

Commands
--------
- **connected**: Determines if elasticsearch server is available.

Dependencies
------------
-   [elasticsearch](https://pypi.python.org/pypi/elasticsearch/1.4.0)
