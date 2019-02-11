# Map Reduce

Steps to Run:

1. python3 master.py config.json
2. python3 tester.py config.json

The master should be running, for the tester to run. Once the master is running, the tester creates a cluster, and runs a
map reduce job as per the config file.
The default config file options initialize a cluster of four mappers and reducers, runs map reduce for inverted index on the
documents sherlock.txt and hound.txt and compares the output to sequential execution of the same job.

More details can be found in /report/report.pdf
