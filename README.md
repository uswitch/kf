# kf

A tool to consume and display Kafka messages. Compatible with Kafka 0.8 and up.

## Usage

    $ kf --help
    usage: kf [<flags>] [<topic>]

    Flags:
      --help               Show help.
      -z, --zookeeper="localhost:2181"
                           host:port of zookeeper server (defaults to value of ZOOKEEPER environment variable)
      -l, --list           list available topics and quit
      -v, --verbose        log informative messages (including offsets)
      -m, --max=0          maximum number of messages before stopping (0 = never stop)
      -b, --from-beginning  get messages from the beginning
      -o, --offset=0       get messages from this offset (0 = get only new messages)
      -g, --group="kf"     consumer group
      -c, --clientid="kf"  client id
      -p, --partition=0    partition

    Args:
      [<topic>]  topic to listen for

    $ export ZOOKEEPER=localhost:2181
    $ kf sometopic
    {"testing":"hello"...

    $ kf -l
    sometopic
    topic1
    topic2
    ...

## Prerequisites

#### Go

To install on Ubuntu

    $ sudo apt-get install golang

To install on OSX

    $ brew install go

Remember to set up a workspace and set GOPATH (see https://golang.org/doc/code.html)

It might also be a good idea to put `$GOPATH/bin` in your `PATH`.

### Mercurial

To install on Ubuntu

    $ sudo apt-get install mercurial

To install on OSX

    $ brew install mercurial

### Bazaar

To install on Ubuntu

    $ sudo apt-get install bzr

To install on OSX

    $ brew install bazaar

### Zookeeper libs/headers

To install on Ubuntu

    $ sudo apt-get install libzookeeper-mt2 libzookeeper-mt-dev

To install on OSX

    $ brew install zookeeper

## Installation

To install on Ubuntu

    $ go get -u github.com/uswitch/kf

To install on OSX:

    $ export CGO_CFLAGS='-I/usr/local/include/zookeeper'
    $ export CGO_LDFLAGS='-L/usr/local/lib'
    $ go get -u github.com/uswitch/kf

## Why?

kf (versus kafka-console-consumer.sh):

* starts up very quickly
* allows you to set an offset from which to receive a message
* allows you to get a list of available topics
* is 23 characters shorter to type!
