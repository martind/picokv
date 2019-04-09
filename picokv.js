'use strict';

// This is just boilerplate, tutorial starts below :-)
const util = require('util')
const fs = require('fs')
const EventEmitter = require('events').EventEmitter

const read = util.promisify(fs.read)
const append = util.promisify(fs.appendFile)
const open = util.promisify(fs.open)
const close = util.promisify(fs.close)
const unlink = util.promisify(fs.unlink)

//       _
// _ __ (_) ___ ___
// | '_ \| |/ __/ _ \
// | |_) | | (_| (_) |
// | .__/|_|\___\___/
// |_|
//  _
// | | ____   __
// | |/ /\ \ / /
// |   <  \ V /
// |_|\_\  \_/
//
//
// A naive key-value store engine inspired by bitcask.
//
//
// Hi!
// Today, we're going to build a persistent key-value store engine.
// It will be a very simple engine, there will be a lot of
// cutting corners and little error checking, so please don't use
// it in production or to store valuable data.
//
// Done with the disclaimer, let's get to it!
//
// So, a key-value store, even a minimalistic one like ours,
// needs to do two things: store some data and retrieve said data.
// Our engine will have a very simple interface, something like this:
//
//     const kv = new PicoKV()
//     kv.set('fnord', '23')
//     console.log(kv.get('fnord')) // prints '23'
//
// We are going to persist data on disk, so that we can retrieve it
// even after a crash or system failure.
//
// Actually, let's start from this. How do we lay out data on disk?
// We are going to store our data in binary format. This will be
// simpler, more performant and will avoid nasty encoding issues.
//
// +----------------+---------------------+-----------------------
// |                |                     |
// |    Header      |         Key         |          Value
// |                |                     |
// +----------------+---------------------+-----------------------
//
// ^                ^                     ^
// |____4 bytes_____|______20 bytes_______|______variable size____
//
// 'Header' will encode the length in bytes of 'Key' + 'Value',
// so that we know when to stop reading while retrieving the value of
// a key or rebuilding an index.
//
// For each 'Key' is reserved a 20 bytes space.
//
// The 'Value' part is of variable size so we can store any value we want.
//
// Let's write some code to represent this structure.

// These are some constants representing 'Header' and 'Key' length,
// respectively 4 and 20 bytes.
const HEADER_LENGTH = 4
const KEY_LENGTH = 20

// Then, we need a couple of helper functions: pack and unpack.
//
// Given a key and a value, 'pack' will allocate and return a buffer
// with the structure discussed above.
function pack(key, value) {
    // some space for the key is allocated
    const keyBuffer = Buffer.alloc(KEY_LENGTH)

    // and the key is written in that buffer
    keyBuffer.write(key)

    // then we convert the value in a buffer
    const valueBuffer = Buffer.from(value)

    // and we concatenate the key buffer with the value buffer
    const keyValueBuffer = Buffer.concat([keyBuffer, valueBuffer])

    // we can now allocate some space for the length of the new packet
    const header = Buffer.alloc(HEADER_LENGTH)

    // and we can write the length of the concatenated key and value
    header.writeUInt32BE(keyValueBuffer.length, 0)

    // a new buffer with header + key + value is returned
    return Buffer.concat([header, keyValueBuffer])
}

// 'unpack' will retrieve a value given a file descriptor and a position inside
// the file. This signature is weird, isn't it? Why it needs a file descriptor and
// a position in that file? Why not a key?
//
// This is related with how we will indexing our data and it will be more clear later.
async function unpack(fd, position) {
    // first we read the length of the packet and store it in a buffer
    const headerBuffer = Buffer.allocUnsafe(HEADER_LENGTH)
    await read(fd, headerBuffer, 0, HEADER_LENGTH, position)

    // we convert the length in an integer
    // 'Header' is encoded in big endian
    const length = headerBuffer.readUInt32BE(0)

    // we read 'length' worth of bytes starting from the position given as parameter
    const contentBuffer = Buffer.allocUnsafe(length)
    await read(fd, contentBuffer, 0, length, position + HEADER_LENGTH)

    // the buffer we just read contains the key along with the value
    // so we need to slice the key out
    const value = contentBuffer.slice(KEY_LENGTH).toString()

    // now we are left with just the value of the key
    return value
}

// Now it's time to talk about our strategies to store and retrieve our data.
//
// Writing is very simple: we just append our data at the end of a file.
// That's it! When some data comes in, it get appended on a file.
// When we need the value of some key we can just read the last entry for
// that key in order to retrieve its value. So, our data file will look something
// like this:
//
//     +----------------+
//     |  foo  ->  bar  |
//     +----------------+
//     |  baz  ->  paz  |
//     +----------------+
//     |  foo  ->  42   |   <-- value of 'foo' is now 42
//     +----------------+
//     |  ...  ->  ...  |
//
// Ok, but what about old key-value pairs? They are not going to be updated.
// Aren't we going to waste a lot of space? Yes, we are! Good catch!
// We'll deal with that in a moment.
//
// Let's talk about reading now. How do we retrieve the last value of a given key?
// We could search for the first entry of said key in the file, starting from the end.
// That would be a simple options. But it's slow. Worst case scenario we could search
// in the whole file.
// We can do a lot better than this by keeping an additional data structure which keep
// track of the last position of every single key in the file.
// Our index will look like this:
//
//          Index
//     +---------------+
//     |  ...          |                          Data file
//     +---------------+                      +----------------+
//     |  foo  ->  0   | -------------------> |  foo  ->  bar  |
//     +---------------+                      +----------------+
//     |  baz  -> 33   |                      |  ...           |
//     +---------------+                      +----------------+
//            |                               |  ...           |
//             -----------------------------> +----------------+
//                                            |  bar  ->  paz  |
//                                            +----------------+
//
// So, by looking in the index we know we can find the value for 'foo' at offset 0
// in the file and the value for 'baz' at offset 33.
// Every time we have to write something we just update our index with the most recent
// position of the given key.
//
// This approach is still simple but more performant than the previous approach of
// brute forcing our way into the data file looking for a key.
// But it has its drawbacks too. The major drawback is that we need enough memory to
// store every distinct key in our datastore.
//
// We are not going to overcome this in this tutorial, so keep that in mind! :-)
//
// Back to our question of wasted space. How do we manage to not run out of space on disk?
// One way to solve this is to separate data in files of a certain size.
// We call each of these files 'segments'.
// Each segment has its own index, as described above.
// When we look for a key we can search through all indexes, starting from the most recent.
// When we need to write down some data and the segment we're writing to gets too big,
// we create a new segment.
//
// We can then schedule a job that goes through all the segments' indexes except the
// current one, get the latest value for each key and create a new compacted segment
// file without the obsolete values.
// The old files could now be just deleted, they are not useful anymore.
// We'll see the details of this process soon.
//
// Enough with the talk, let's write some code for all this stuff!
//
//
// Here we define some default values for maximum segment size and how frequently the
// engine should try to compact old segments. Defaults are 500 kb and 10 seconds respectively.
const DEFAULT_MAX_SEGMENT_SIZE = 500 * 1024
const DEFAULT_COMPACTION_INTERVAL = 10 * 1000

// Here's our main class PicoKV. It extends EventEmitter so we can listen to its event, like
// when a compaction starts or a key is set.
class PicoKV extends EventEmitter {
    constructor(options) {
        super()

        // Default values can be overridden
        var options = options || {}
        this._segmentsSize = options.segmentsSize || DEFAULT_MAX_SEGMENT_SIZE
        this._compactionInterval = options.compactionInterval || DEFAULT_COMPACTION_INTERVAL

        // This is where all our segments will be written on disk
        this._dbPath = './db/'

        // And here we will store all the indexes for our segments
        // remember that each segment will have its own index
        this._indexes = []

        // We need to rebuild the indexes from segments' data on disk is available
        // we'll get to it later
        this._loadExistingData()

        // We also schedule a job that will look for compaction of old segments
        this._setCompactionJob()
    }

    // This is our get method that will return a value given a key
    // or null if no such key exists
    async get(key) {
        // We store the file descriptor of the segment that contain our key here
        let fd;

        // And its position on that file here
        let keyPosition;

        // We start looking for a key starting from the most recent index
        // and going backwards
        for (let i = 0; i < this._indexes.length; i++) {
            const index = this._indexes[i]
            keyPosition = index.data.get(key)

            // If we have found the key we'll have a valid offset
            if (keyPosition !== undefined) {
                // so we can get a reference to the file containing the key
                fd = index.fd

                // and stop the loop
                break;
            }
        }

        // If a position has not been found, the key is not in the store
        if (keyPosition === undefined) return null

        // We can now unpack the value from the appropriate segment
        // at the correct position
        return await unpack(fd, keyPosition)
    }

    // and this is our set method that append a key-value pair in the current segment
    async set(key, value) {
        // We get the current index
        // indexes are ordered from the most recent to the oldest
        let index = this._indexes[0]

        // If we reached the segment size limit we need to create a new file
        // and a new index to write to
        if (index.lastPosition > this._segmentsSize) {
            // We append a timestamp in the file name
            // this is arbitrary, and is simpler than having a sequence number
            const segment = `${this._dbPath}picokv-${new Date().getTime()}.pkv`

            // A new index needs to be created for the new segment
            // here's the anatomy of the index
            index = {
                segment, // we have a reference to the segment's name
                fd: fs.openSync(segment, 'a+'), // a file descriptor of the segment
                data: new Map(), // a Map which keep the actual key to offset mapping
                lastPosition: 0 // and the last offset we used
                                // this is used to check the segment' size
                                // and to keep track of the last offset used
            }

            // This will become our current index
            // so we need to put it in front of the other indexes
            this._indexes.unshift(index)
        }

        // Now we can pack a buffer for the key-value pair
        const content = pack(key, value)

        // That can be simply appended at the end of
        // the file pointed by the most recent index
        await append(index.fd, content)

        // Keep track of the position of the key inside the segment
        index.data.set(key, index.lastPosition)

        // And update the last used offset
        index.lastPosition += content.length

        // We emit an event after successfully writing a key-value pair
        // because it's nice to have :-)
        this.emit('setkey', key, value)
    }

    // Ok, up until now everything works if we keep our process running.
    // But what happens if some system failure happens? Like our process die or
    // the kernel panics.
    // Data would be safe, because we write all the data to disk (unless we
    // experience a disk failure and we have no backups, but that's another story).
    // But all our indexes would be gone, because we keep them in memory.
    //
    // We need to rebuild our indexes from the segments we have on disk. This
    // method is called in the class constructor and is run only we you
    // create an instance of PicoKV.
    _loadExistingData() {
        // This method is also used for initialization if we start with nothing
        if (!fs.existsSync(this._dbPath)) {
            fs.mkdirSync(this._dbPath)
        }

        // Here we will keep the segments we find on disk
        let segments = []

        // We have to deal with two kind of segments: compacted and normal
        // we use two different file extensions to identify those
        //
        // .pkvc are compacted segments
        // .pkv are normal segments
        //
        // We need to load compacted segments first because they are filled
        // with possibly old data and need to be checked after normal segments,
        // which may contain the most recent value for a given key
        for (const extension of ['pkvc', 'pkv']) {
            segments = segments.concat(fs.readdirSync(this._dbPath)
                .filter(file => file.endsWith(extension)) // we consider one extension at a time
                .map(segment => ({
                    file: segment,
                    // for each file we retrieve the last modification date
                    mtime: fs.statSync(`${this._dbPath}${segment}`).mtime
                }))
                .sort((a, b) => a.mtime.getTime() - b.mtime.getTime()) // we sort files by date
                .map(segment => `${this._dbPath}${segment.file}`)) // and we just keep the file's name, we don't need the date anymore
        }

        // If we find no normal segments, maybe we start from scratch so we create one
        if (!segments.some(segment => segment.endsWith('.pkv'))) {
            const segment = `${this._dbPath}picokv-${new Date().getTime()}.pkv`
            fs.closeSync(fs.openSync(segment, 'w'))
            segments.push(segment)
        }

        // For each segment we have to rebuild its index
        for (const segment of segments) {
            const data = fs.readFileSync(segment)
            const fileSize = data.length

            let index = {
                segment,
                fd: fs.openSync(segment, 'a+'),
                data: new Map(),
                lastPosition: fileSize
            }

            // We consume the whole file
            let position = 0
            while (position < fileSize) {
                // Reading each key-value buffer
                const length = data.readIntBE(position, HEADER_LENGTH)

                // Unpacking just the key
                const key = data.slice(position + HEADER_LENGTH, position + HEADER_LENGTH + KEY_LENGTH)
                                .toString()
                                .replace(/\0/g, '') // removing the NULL bytes

                // Updating the key offset in our index
                index.data.set(key, position)

                // And incrementing the position reaching the next key
                position += length + HEADER_LENGTH
            }

            // Indexes will be collected in a list in reverse order so that
            // the first one is the index of the most recent segment
            this._indexes.unshift(index)
        }
    }

    // Now take a deep breath, we're almost done! :-)
    //
    // Here is our compaction routine that keeps only the most recent values
    // for each key in our set and deletes obsolete segments
    _setCompactionJob() {
        setInterval(async () => {
            // First we need an index that will contain the most recent position
            // of every key
            const distinctOldKeys = new Map()

            // But we're going to exclude the current segment, the one we're
            // writing to. That is not a problem because the index of the
            // current segment will be kept as the first index we check
            // when we look for a key.
            // Here, we also start from the oldest index we have, because
            // we could have a more recent value for a given key in a newer segment.
            for (let i = this._indexes.length - 1; i > 0; i--) {
                const index = this._indexes[i]

                // we keep track of index's data
                const fd = index.fd
                const oldIndex = index.data
                const segment = index.segment

                // For every key in the index we keep track of its current offset,
                // segment and file descriptor.
                // At the end of the loop we will end up with the most recent
                // positions of every key.
                for (let [key, position] of oldIndex) {
                    distinctOldKeys.set(key, { fd, position, segment })
                }
            }

            // Maybe we have to file to examine, so we have no compaction to do
            if (distinctOldKeys.size === 0) return

            // Now we can create our compacted segment.
            // Note that in our implementation compacted segments do not follow
            // the maximum segment limit like normal segment do. This is by choice,
            // in order to keep the implementation straightforward
            //
            // Compacted segments will have a .pkvc extension as we saw earlier
            const segment = `${this._dbPath}picokv-${new Date().getTime()}.pkvc`
            const fd = await open(segment, 'a+')

            // Compacted files will have their index, of course
            const compactedIndex = {
                fd,
                segment,
                data: new Map(),
                lastPosition: 0
            }

            // For each key we found
            for (let [key, data] of distinctOldKeys) {
                // We unpack the key's value for the correct segment
                const value = await unpack(data.fd, data.position)

                // We repack the value in a new buffer
                const content = pack(key, value)

                // And append it to the compacted segment we're creating
                await append(fd, content)

                // Finally we can update the index for the compacted segment
                compactedIndex.data.set(key, compactedIndex.lastPosition)
                compactedIndex.lastPosition += content.length
            }

            // Now we can put our new file to good use by just inserting its
            // index in the list of searchable indexes.
            // Note that up until now no read has been blocked because we were
            // still using the obsolete segments we have just compacted.
            // We insert the new index in second position, just before the
            // index of the current segment we are writing to, removing all the
            // other indexes at the same time thanks to Array.splice() function.
            const obsoleteIndexes = this._indexes.splice(1, this._indexes.length - 1, compactedIndex)

            // We can now safely delete every obsolete segment
            obsoleteIndexes.forEach(async idx => {
                await close(idx.fd)
                unlink(idx.segment)
            })

            // And emit a nice event at the end of the whole process
            const deletedIndexes = obsoleteIndexes.map(idx => idx.segment)
            this.emit('compacted', deletedIndexes, segment)
        }, this._compactionInterval)
    }
}

// I hope you enjoyed the journey so far, I know it has been quite a ride for me
// to write it in the first place :-)
//
// With the last line we export our PicoKV class, and now we have our own naive
// on-disk key-value store.
//
// The End.
module.exports = PicoKV

