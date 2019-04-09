// Ok, this file is basically for testing purposes and usage example
// We expose get/set functions through a simple HTTP layer using express
const express = require('express')
const bodyParser = require('body-parser')
const PicoKV = require('./picokv')

const PORT = 9001
const api = express()

// We can override a couple of parameters in order to customize the interval between compactions
// and the maximum size of segments on disk.
const kv = new PicoKV({
    compactionInterval: 20 * 1000,
    segmentsSize: 10 * 1024
})

kv.on('compacted', (oldFiles, newFile) =>
    console.log('Some files', JSON.stringify(oldFiles), 'have been compacted into', newFile))

api.use(bodyParser.text({type:"*/*"}))

api.get('/pkv/:key', async (req, res) => {
    const value = await kv.get(req.params.key)
    res.send(value)
})

api.put('/pkv/:key', async (req, res) => {
    await kv.set(req.params.key, req.body)
    res.send('OK')
})

api.listen(PORT, () => console.log(`picokv server listening on port ${PORT}`))

