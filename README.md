# import-large-xml-into-mdb
Provides a Python program to load large XML files into MongoDB where the XML file size is larger than the host's RAM. Takes an input XML file and for each repeating XML element (e.g. for `ITEM` in `<ROOT><ITEMS><ITEM>`) inserts a JSON/BSON document into a MongoDB database collection.

## Prerequisites
Install Python XML & MongoDB Driver (PyMongo) libraries, eg:

    $ pip3 install --user pymongo lxml

## Executing
For usage first ensure the '.py' script is executable and then run:

    $ ./import-xml-into-mdb.py -h

Example:

    $ ./import-xml-into-mdb.py -f 'data/mydoc.xml' -r 'items/item' -d mydb -u 'mongodb+srv://usr:pwd@mycluster-a123z.mongodb.net'
    ```

## Further Information

Works with very large XML files (eg. 10s GBs or more in size) regardless of how much RAM the host machine has by streaming the XML file in, and for each repeating XML branch processed, the XML branch is cleared from memory, before reading the next branch in, and so on. An earlier incarnation of this program used the common Python XML processing library, 'xmltodict', which would cause the program to crash for very large XML files with out of memory errors, due to it attempting to read the whole XML file into RAM all in one go.

Loads a batch of 1000 JSON/BSON documents at a time into the MongoDB collection to decrease ingestion time. However, does not [currently] attempt to use multi-threading to ingest portions of XML and insert into MongoDB, in parallel.

Because it is assumed that XML Schemas (XSDs/DTDs) are not defined for the XML file being read, the program has to make conservative assumptions about how to structure each resulting a JSON/ BSON document based on the repeating XML branch. Specifically each identified XML sub-element ALWAYS needs to be held in a JSON array. This is because for XML, even if an element has just a text value, another element with the same name could occur again at the same level, containing some other text, even if in the specific sample XML file provided in the specific situation doesn't actually occur.

For example the repeating XML branch:

     <thing><name>Bob</name></thing>

 ..is translated to the JSON/BSON of:


    {'thing': [{'name': ['Bob']}]}

 ..and not:

    {'thing': {'name': 'Bob'}}

 ..because in some cases `<name/>` could legally appear twice in `<thing/>`, and additionally` <thing/>` could legally contain another type of element too, such as in the following example XML branch:

     <thing><name>Bob</name><type>Monster</type><name>Alice</name></thing>

 ..which would be translated to the JSON/BSON of:

     {'thing': [{'name': ['Bob', 'Alice'], 'type': ['Monster']}]}

The alternative could be to not use an array any time only a single child element occurs. However this would lead to inconsistencies across ingested MongoDB documents. MongoDB users would need to query collections in two ways (one filter assuming arrays, one not) to find one specific element. For example, the following two XML elements would otherwise result in the two different JSON/BSON structures, requiring two different types of queries, if this program chose not to wrap single child elements in arrays:

     <sublabels> <label>Action Records</label> </sublabels>
     <sublabels> <label>Action Records</label> <label>Fizz Music</label> </sublabels>

Performance of this program is based on a number of factors. The program does NOT use multi- threading to process sections of XML and insert into MongoDB. However it does insert large batches of records, at a time, into MongoDB which helps performance. On a modern multi-core laptop with SSD storage, running both this program and a single node MongoDB instance on the same machine, an ingest rate of approximately 40k records is achievable (for records of size 181 bytes on average, with no secondary indexes defined).


