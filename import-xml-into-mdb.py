#!/usr/bin/python3
##
# Program to load large XML files into MongoDB where the XML file size is larger than the host's
# RAM. Takes an input XML file and for each repeating XML element (e.g. for 'ITEM' in
# <ROOT><ITEMS><ITEM>) inserts a JSON/BSON document into a MongoDB database collection.
#
#
# Prerequisites:
# * Install Python XML & MongoDB Driver (PyMongo)  libraries, eg:
#  $ pip3 install --user pymongo lxml
#
#
# For usage first ensure the '.py' script is executable and then run:
#  $ ./import-xml-into-mdb.py -h
#
# Example:
#  $ ./import-xml-into-mdb.py -f 'data/mydoc.xml' -r 'items/item' -d mydb
#                             -u 'mongodb+srv://usr:pwd@mycluster-a123z.mongodb.net'
#
#
# Works with very large XML files (eg. 10s GBs or more in size) regardless of how much RAM the
# host machine has by streaming the XML file in, and for each repeating XML branch processed, the
# XML branch is cleared from memory, before reading the next branch in, and so on. An earlier
# incarnation of this program used the common Python XML processing library, 'xmltodict', which
# would cause the program to crash for very large XML files with out of memory errors, due to it
# attempting to read the whole XML file into RAM all in one go.
#
# Loads a batch of 1000 JSON/BSON documents at a time into the MongoDB collection to decrease
# ingestion time. However, does not [currently] attempt to use multi-threading to ingest portions
# of XML and insert into MongoDB, in parallel.
#
# Because it is assumed that XML Schemas (XSDs/DTDs) are not defined for the XML file being read,
# the program has to make conservative assumptions about how to structure each resulting a JSON/
# BSON document based on the repeating XML branch. Specifically each identified XML sub-element
# ALWAYS needs to be held in a JSON array. This is because for XML, even if an element has just a
# text value, another element with the same name could occur again at the same level, containing
# some other text, even if in the specific sample XML file provided in the specific situation
# doesn't actually occur.
#
# For example the repeating XML branch:
#     <thing><name>Bob</name></thing>
# ..is translated to the JSON/BSON of:
#     {'thing': [{'name': ['Bob']}]}
# ..and not:
#     {'thing': {'name': 'Bob'}}
# ..because in some cases <name/> could legally appear twice in <thing/>, and additionally <thing/>
#   could legally contain another type of element too, such as in the following example XML branch:
#     <thing><name>Bob</name><type>Monster</type><name>Alice</name></thing>
# ..which would be translated to the JSON/BSON of:
#     {'thing': [{'name': ['Bob', 'Alice'], 'type': ['Monster']}]}
#
# The alternative could be to not use an array any time only a single child element occurs.
# However this would lead to inconsistencies across ingested MongoDB documents. MongoDB users would
# need to query collections in two ways (one filter assuming arrays, one not) to find one specific
# element. For example, the following two XML elements would otherwise result in the two different
# JSON/BSON structures, requiring two different types of queries, if this program chose not to wrap
# single child elements in arrays:
#     <sublabels> <label>Action Records</label> </sublabels>
#     <sublabels> <label>Action Records</label> <label>Fizz Music</label> </sublabels>
#
# Performance of this program is based on a number of factors. The program does NOT use multi-
# threading to process sections of XML and insert into MongoDB. However it does insert large
# batches of records, at a time, into MongoDB which helps performance. On a modern multi-core
# laptop with SSD storage, running both this program and a single node MongoDB instance on the same
# machine, an ingest rate of approximately 40k records is achievable (for records of size 181 bytes
# on average, with no secondary indexes defined).
##
import argparse
from lxml import etree
from pymongo import MongoClient
import time
from pprint import pprint


##
# Main function to parse passed-in process before invoking the core processing function
##
def main():
    argparser = argparse.ArgumentParser(description='Reads an XML file which contains repeating '
                                        'elements directly under the root XML node importing '
                                        'each into MongoDB as part of a repeating bulk insert')
    argparser.add_argument('-f', '--file', required=True,
                           help=f'The path of the XML file to import ')
    argparser.add_argument('-r', '--repeatingpath', required=True,
                           help=f'The slash separated path of the repeating XML element in the XML'
                           'file to iterate through, where there must be at least two XML element'
                           'parts to the path and the first part is the single root element of the'
                           'XML file. For example, specify the path of "DATA/ITEMS/ITEM" for the'
                           'section of XML "<DATA><ITEMS><ITEM>" where "<DATA>" it the root node'
                           'and <ITEMS> is the significant repeating node to iterate')
    argparser.add_argument('-i', '--ignore',
                           help=f'The comma separated names of XML elements hanging off the '
                           'repeating element (e.g. XXX,YYY) which should be ignored and not '
                           'included in MongoDB document')
    argparser.add_argument('-d', '--database', default=DEFAULT_DB_NAME,
                           help=f'MongoDB database to insert into (default: {DEFAULT_DB_NAME})')
    argparser.add_argument('-c', '--collection',
                           help=f'MongoDB collection to insert into (default: the name of the XML '
                           'element which is the parent to the last element in the '
                           '"repeatingpath")')
    argparser.add_argument('-u', '--url', default=DEFAULT_MONGODB_URL,
                           help=f'MongoDB Cluster URL (default: {DEFAULT_MONGODB_URL})')
    args = argparser.parse_args()

    axis_path = [itm.strip() for itm in args.repeatingpath.split('/')]

    if len(axis_path) < 2:
        sys.exit(f"Variable '-r'/'--repeatingpath' must contain at least two elements separated "
                 "by a slash; value specified is: {'/'.join(axis_path)}")

    if not args.collection:
        args.collection = axis_path[-2]

    if args.ignore:
        ignore_list = [itm.strip() for itm in args.ignore.split(',')]
    else:
        ignore_list = []

    run(args.file, axis_path, ignore_list, args.url, args.database, args.collection)


##
# Get connection to MongoDB database and collection then kick off main XML processing and ingestion
# process
##
def run(xml_filepath, axis_path, ignore_list, url, dbname, collname):
    start = time.time()
    print(f"Processing XML repeating element '{'/'.join(axis_path)}' in XML file '{xml_filepath}'")
    print(f'Opening MongoDB connection to: {url}')
    connection = MongoClient(host=url)
    db = connection[dbname]
    coll = db[collname]
    coll.drop()
    insert_array_elements_into_db(xml_filepath, axis_path, ignore_list, coll)
    print(f'Ingest duration: {int(time.time() - start)} secs')


##
# Process each XML repeating branch at a time, clearing it from memory to save RAM, then batching
# sets of resulting documents into groups, ingesting each batch into a MongoDB collection (and then
# removing from RAM, before building out the next batch to process, and so on
##
def insert_array_elements_into_db(xml_filepath, axis_path, ignore_list, coll):
    count = 0
    records_batch = []
    repeating_xml_name = axis_path[-1]
    context = etree.iterparse(xml_filepath, events=('end',), tag=repeating_xml_name)

    for event, elem in context:
        if is_matching_repeating_element(axis_path, elem):
            record_dict = recurse_element_descending(elem, ignore_list, 1)

            if record_dict:
                if type(record_dict) is not dict:
                    record_dict = {'val': record_dict}

                # pprint(record_dict)
                records_batch.append(record_dict)

                if count % MDB_BATCH_INSERT_SIZE == 0:
                    coll.insert_many(records_batch)
                    records_batch = []

                if count % INSERT_COUNT_STATUS_THRESHOLD == 0 and count > 0:
                    print(f'- Records inserted: {count}')

                elem.clear()  # Clear this XML branch as it will no longer be needed
                count += 1

    if records_batch:
        coll.insert_many(records_batch)
        records_batch = []

    print(f'- Records inserted: {count}')
    del context


##
# Taking an XML element recursively process its child XML elements adding them as a list of values
# and then for the current XML element capture its attributes and text content returning appear
##
def recurse_element_descending(elem, ignore_list, depth):
    result = {}

    for child_elem in elem.getchildren():
        child = recurse_element_descending(child_elem, ignore_list, depth+1)

        if child and (depth > 1 or child_elem.tag not in ignore_list):
            # print(f'parent: {elem.tag},tag: {child_elem.tag},type: { type(child)},val: {child}')

            if child_elem.tag not in result:
                result[child_elem.tag] = [child]
            else:
                result[child_elem.tag].append(child)

    for attrname, attrval in elem.items():
        set_unique_key_val_if_exists(result, attrname, attrval)

    if elem.text and elem.text.strip():
        if result:
            set_unique_key_val_if_exists(result, VALUE_FIELD_PREFIX, elem.text)
        else:
            result = elem.text

    return result


##
# Ensures the key doesn't exist in the dictionary before inserting it, by prefixing with one or
# more underscores if it won't otherwise be unique
##
def set_unique_key_val_if_exists(record_dict, key, value):
    if key and value:
        keyfield = key

        while keyfield in record_dict:
            keyfield = f'_{keyfield}'

        record_dict[keyfield] = value


##
# Walks the specific axis path in reverse order whilst walking the XML element's ancestry to check
# if they match exactly up to the root of the XML document
##
def is_matching_repeating_element(axis_path, elem):
    curr_elem = elem

    for section in reversed(axis_path):
        if curr_elem is None or section != curr_elem.tag:
            return False

        curr_elem = curr_elem.getparent()

    if curr_elem is not None:
        return False

    return True


# Constants
DEFAULT_MONGODB_URL = 'mongodb://localhost:27017'
DEFAULT_DB_NAME = 'test'
MDB_BATCH_INSERT_SIZE = 1000
INSERT_COUNT_STATUS_THRESHOLD = 100000
VALUE_FIELD_PREFIX = 'val'


##
# Main
##
if __name__ == '__main__':
    main()
