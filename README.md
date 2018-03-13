# What?
Really, really, fast S3 bucket cleanup, including versions.

# Why?
Because the official CLI does not delete object versions, and https://github.com/eschwim/s3wipe turned out too slow still.

# How?
Clone and `go run main.go -region us-east-2 -bucket your-bucket`. 
**There will be no confirmation.**  
The bucket will be deleted once all files have been removed.

*[UNTESTED]:* Pass `-prefix some/path/to/files` to scope the deletion.  Bucket will *NOT* be deleted at the end if a prefix is specified.

# Other
Page sizes for both list and delete requests is the default (1000).

Up to 16 (default) concurrent deletes happen by default.  Increase with `-workers XXX` if `queued:` is non-zero.

AWS errors are not handled; restart the process (or send a PR!).
