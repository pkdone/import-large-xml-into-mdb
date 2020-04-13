#!/bin/sh
#
# EXAMPLES
#

# IMPORT TO SOME SAMPLE MUSIC ARTIST DATA FROM DISCOGS.COM
./import-xml-into-mdb.py -f 'test/artists-test.xml' -r 'artists-behind-my-top-twelve-albums/artists/artist' -i 'images' -d testdb

# IMPORT TO MONGODB USING VARIOUS TEST XML FILES
#./import-xml-into-mdb.py -f 'test/simple.xml' -r 'labels/label' -d testdb -c simple
#./import-xml-into-mdb.py -f 'test/thing1.xml' -r'data/main' -d testdb -c thing1
#./import-xml-into-mdb.py -f 'test/thing2.xml' -r'data/main' -d testdb -c thing2
#./import-xml-into-mdb.py -f 'test/test.xml' -r 'labels/label' -d testdb -c test
#./import-xml-into-mdb.py -f 'test/labels.xml' -r 'labels/label' -i 'images' -d testdb
#./import-xml-into-mdb.py -f 'test/deeper-labels.xml' -r 'main-doc/main-data/labels/label' -i 'images' -d testdb -c deeper_labels
#./import-xml-into-mdb.py -f 'test/why_arrar_for_apparent_str.xml' -r 'root/labels/label' -i 'images' -d testdb -c repeater_labels

# IMPORT THE 4 MAIN COMPLETE DATA SETS FROM https://www.discogs.com/ (NEEDS TO BE DOWNLOADED FROM https://data.discogs.com/ TO 'data' SUB-FOLDER AND UNZIPPED FIRST)
#./import-xml-into-mdb.py -f 'data/discogs_20200301_labels.xml' -r 'labels/label' -i 'images' -d music
#./import-xml-into-mdb.py -f 'data/discogs_20200301_artists.xml' -r 'artists/artist' -i 'images' -d music
#./import-xml-into-mdb.py -f 'data/discogs_20200301_masters.xml' -r 'masters/master' -i 'images' -d music
#./import-xml-into-mdb.py -f 'data/discogs_20200301_releases.xml' -r 'releases/release' -i 'images' -d music

