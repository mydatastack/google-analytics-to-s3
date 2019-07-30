const { describe } = require('riteway')
const handler = require('./index.js').handler
const payload = require('./payload.json')

describe('handler()', async assert =>
  assert({
    given: 'json firehose payload',
    should: 'decode base64, parse user_agent and decode it back',
    actual: await handler(payload).then(x => Object.keys(x)),
    expected: ['records'] 
  })
)
