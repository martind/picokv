# PicoKV

A naive persistent key-value store for learning purposes.

## What

PicoKV is a small and simplistic persistent key-value store made available in as an heavily commented source code file for learning purposes. It is **NOT** meant for production use or for storing valuable data.

However, if you are curious about how stuff like Redis, Riak or LevelDB work, this might be a nice starting point.

Start reading the code [here](picokv.js).

## Why

> What I cannot create, I do not understand. -- *Richard Feynman*

I was reading *Designing Data-Intensive Applications* by Martin Kleppmann and I wanted to learn more about the algorithms described in the book by writing a naive implementation of the simplest on-disk key-value store.

PicoKV is an implementation of the Hash Index algorithm in Chapter 3 and it resembles the approach of Bitcask.

## Run

You can use PicoKV as a library in your code, like [this](server.js). Or you can run a simple HTTP server that accept GET/PUT requests on top of it with `make run`.

Example:

```
$ make run
$ curl -s -d "foobar" -H "content-type: text/plain" -X PUT http://localhost:9001/pkv/foo
OK
$ curl -s -X GET http://localhost:9001/pkv/foo
foobar
```

## Tests

Run `make test`. You'll need docker.

## More

Want more? Cool! Get a copy of *Designing Data-Intensive Applications* by Martin Kleppmann. It's a **great** book!

You can also dig into the actual source code of more comprehensive projects such as:

[Bitcask](https://github.com/basho/bitcask)

[LevelDB](https://github.com/google/leveldb)

[Redis](https://github.com/antirez/redis)

## :neckbeard: Hey man, this sucks... you have no clue about what you're doing!

You're most likely right! Feel free to submit a pull request! :wink:

## Credits

The style of the tutorial is inspired by the [Super Tiny Compiler](https://github.com/jamiebuilds/the-super-tiny-compiler) and [jonesforth](http://git.annexia.org/?p=jonesforth.git;a=blob;f=jonesforth.S;h=45e6e854a5d2a4c3f26af264dfce56379d401425;hb=66c56998125f3ac265a3a1df9821fd52cfeee8cc). Check them out, they are great!

