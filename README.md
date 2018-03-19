# What?
Really, really, fast S3 bucket cleanup, including versions.

# Why?
Because the official CLI does not delete object versions, and https://github.com/eschwim/s3wipe turned out too slow still.

# How?
```
go install github.com/sgrankin/s3-purge-bucket
s3-purge-bucket -region us-east-2 bucket1 bucket2...
```
*There will be no confirmation.*
The bucket will be deleted once all files have been removed.

Pass `-prefix some/path/to/files` to scope the object listing.  The prefix will be used with each specified bucket.
Bucket deletion will be attempted, but may fail if your prefix does not cover all objects. 

Use brace expansion in the prefix to parallelize the object listing and drastically speed up  deletion.
For example, `-prefix 'things/{{a..z},{A..Z},{0..9},-,_}'` will delete base-64 prefixed objects under the prefix `things/`.

# Other
The page sizes for both list and delete API requests is the default maximum (1000).

Increase the number of concurrent deletion workers with `-workers N` if you notice the `queued:` metric constantly high.
