# s3hash

> Download and sha256 hash s3 objects

The S3 hasher is designed to compute SHA256 hashes for objects stored in Amazon S3. It is written in Go and uses Goroutines and channels to stream the input as well as s3 objects, compute the sha256 hashes and write them to a file. S3 objects are only kept in memory and are not stored on the filesystem. This tool is useful for calculating the sha256 hash of large volumes of data stored in an S3 bucket.

This is written as a Proof-of-Concept / Demo and not for production use, but could be adapted fairly easily for that purpose.

## Build

```sh
go build
```

## Usage/Run

```sh
% ./go-s3hash
Error: required flag(s) "bucket", "keys-file" not set
Usage:
  s3hash [flags]

Flags:
      --bucket string        The s3 bucket from which to fetch objects
  -h, --help                 help for s3hash
      --keys-file string     The input file from which to read s3 keys
      --num-threads uint16   The number of threads to use (defaults to NUM_CPUS * 2) (default 32)

% ./go-s3hash --bucket mybucket --keys-file keys.txt
```

## Dependency Updates

To update dependencies:

```sh
go get -u ./...
go mod tidy
```

... should do the trick. Don't forget to track the changes in git!

## LICENSE

Released under the [ISC license](LICENSE)
