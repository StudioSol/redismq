Note: This project is no longer actively maintained. Please refer to its spiritual successor [rmq].

[rmq]: https://github.com/adjust/rmq

--

# redismq

[![Build Status](https://secure.travis-ci.org/adjust/redismq.png)](http://travis-ci.org/adjust/redismq) [![godoc](http://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](https://godoc.org/github.com/adjust/redismq) 

## What is this

This is a fast, persistent, atomic message queue implementation that uses redis as its storage engine written in go.
It uses atomic list commands to ensure that messages are delivered only once in the right order without being lost by crashing consumers.

Details can be found in the blog post about its initial design:
[http://big-elephants.com/2013-09/building-a-message-queue-using-redis-in-go/](http://big-elephants.com/2013-09/building-a-message-queue-using-redis-in-go/)

A second article desribes the performance improvements of the current version:
[http://big-elephants.com/2013-10/tuning-redismq-how-to-use-redis-in-go/](http://big-elephants.com/2013-10/tuning-redismq-how-to-use-redis-in-go/)

## What it's not

It's not a standalone server that you can use as a message queue, at least not for now. The implementation is done purely client side. All message queue commands are "translated" into redis commands and then executed via a redis client.

If you want to use this with any other language than go you have to translate all of the commands into your language of choice.

## How to use it

All most all use cases are either covered in the [examples](https://github.com/adjust/redismq/tree/master/example)
or in the [tests](https://github.com/adjust/redismq/tree/master/test).

So the best idea is just to read those and figure it from there. But in any case:

### Basics

To get started you need a running redis server. Since the tests run `FlushDB()` an otherwise unused database is highly recommended
The first step is to create a new queue:
```go
package main

import (
	"fmt"
	"github.com/adjust/redismq"
)

func main() {
	testQueue := redismq.CreateQueue("localhost", "6379", "", 9, "clicks")
	...
}
```
To write into the queue you simply use `Put()`:
```go
	...
	testQueue := redismq.CreateQueue("localhost", "6379", "", 9, "clicks")
	testQueue.Put("testpayload")
	...
}
```
The payload can be any kind of string, yes even a [10MB one](https://github.com/adjust/redismq/blob/master/test/integration_test.go#L217).

To get messages out of the queue you need a consumer:
```go
	...
	consumer, err := testQueue.AddConsumer("testconsumer")
	if err != nil {
		panic(err)
	}
	package, err := consumer.Get()
	if err != nil {
		panic(err)
	}
	fmt.Println(package.Payload)
	...
}
```
`Payload` will hold the original string, while `package` will have some additional header information.

To remove a package from the queue you have to `Ack()` it:
```go
	...
	package, err := consumer.Get()
	if err != nil {
		panic(err)
	}
	err = package.Ack()
	if err != nil {
		panic(err)
	}
	...
}
```

### Multi Get

Like `MultiGet()` speeds up the fetching of messages. The good news it comes without the buffer loss issues.

Usage is pretty straight forward with the only difference being the `MultiAck()`:
```go
	...
	packages, err := consumer.MultiGet(100)
	if err != nil {
		panic(err)
	}
	for i := range packages {
		fmt.Println(p[i].Payload)
	}
	packages[len(p)-1].MultiAck()
	...
}
```
`MultiAck()` can be called on any package in the array with all the prior packages being "acked". This way you can `Fail()` single packages.

### Reject and Failed Queues

Similar to AMQP redismq supports `Failed Queues` meaning that packages that are rejected by a consumer will be stored in separate queue for further inspection. Alternatively a consumer can also `Requeue()` a package and put it back into the queue:
```go
	...
	package, err := consumer.Get()
	if err != nil {
		panic(err)
	}
	err = package.Requeue()
	if err != nil {
		panic(err)
	}
	...
}
```

To push the message into the `Failed Queue` of this consumer simply use `Fail()`:
```go
	...
	package, err := consumer.Get()
	if err != nil {
		panic(err)
	}
	err = package.Fail()
	if err != nil {
		panic(err)
	}
	package, err = suite.consumer.GetUnacked()
	...
}
```
As you can see there is also a command to get messages from the `Failed Queue`.

## How fast is it

Even though the original implementation wasn't aiming for high speeds the addition of `MultiGet`
make it go something like [this](http://www.youtube.com/watch?feature=player_detailpage&v=sGBMSLvggqA#t=58).

All of the following benchmarks were conducted on a MacBook Retina with a 2.4 GHz i7.
The InputRate is the number of messages per second that get inserted, WorkRate the messages per second consumed.

Single Publisher, Two Consumers only atomic `Get` and `Put`
```
InputRate:	12183
WorkRate:	12397
```

Single Publisher, Two Consumers using `MultiGet`
```
InputRate:	46994
WorkRate:	25000
```

And yes that is a persistent message queue that can move over 70k messages per second.

If you want to find out for yourself checkout the `example` folder. The `load.go`
will start a web server that will display performance stats under `http://localhost:9999/stats`.

## How persistent is it

As redis is the underlying storage engine you can set your desired persistence somewhere between YOLO and fsync().
With somewhat sane settings you should see no significant performance decrease.

## Copyright
redismq is Copyright © 2014 adjust GmbH.

It is free software, and may be redistributed under the terms
specified in the LICENSE file.
