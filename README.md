# streamer
tail files and send them to nats

Launches a go routine for each subject, file pair and shuffles the files along. A single failed file should only error out, not bring it down.

## Running it 

``` shell 
Usage:
   [flags]

Flags:
  -c, --config string   a configruation file to use (default "config.json")
  -d, --debug           enable debug logging
```

## Configuring it

``` json
{
  "nats_conf": {
    "servers": ["nats://nats.lo:4222"],
    "ca_files": ["/usr/local/etc/certs/ca.pem"],
    "key_file": "/usr/local/etc/certs/test-key.pem",
    "cert_file": "/usr/local/etc/certs/test.pem",
    "hostname": "nats.lo"
  },
  "subjects": {
    "test-1-log": "test1.log",
    "test-2-log": "test2.log"
  }
}
```
