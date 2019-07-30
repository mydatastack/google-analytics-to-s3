const pipe = fns => x => fns.reduce((v, f) => f(v), x)
const maxmind = require('maxmind')
const DeviceDetector = require('node-device-detector')
const querystring = require('querystring')
const detector = new DeviceDetector
const userAgent = 'Mozilla/5.0 (Linux; Android 5.0; NX505J Build/KVT49L) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.78 Mobile Safari/537.36';

const result = detector.detect(userAgent)

const b64Decode = b64string => Buffer.from(b64string, 'base64').toString('ascii')
const b64Encode = string => Buffer.from(JSON.stringify(string) + '\n').toString('base64')
const getData = xs => xs.map(e => ({...e, data: JSON.parse(b64Decode(e.data))}))
const parseBody = xs => xs.map(e => ({...e, data: {...e.data, body: querystring.decode(e.data.body)}}))
const getProp =  prop => o => o[prop]
const parseUA = xs => xs.map(e => ({...e, ua_parsed: e.data.user_agent}))
const runDeviceDetector = xs => xs.map(e => e.is_bot 
  ? e
  : ({...e, ua_detected: detector.detect(e.ua_parsed)}))

const runBotDetector = xs => xs.map(e => ({...e, is_bot: detector.parseBot(e.ua_parsed)}))
const markBots = xs => xs.map(e => 
  Object.keys(e.is_bot).length === 0 && e.is_bot.constructor === Object 
    ? ({...e, is_bot: false})
    : ({...e, is_bot: true})

)
const reduceIPdata = e => ({
    city: {
      geoname_id: e.city.geoname_id, 
      name_en: e.city.names.en
    },
    continent: {
      code: e.continent.code,
      geoname_id: e.continent.geoname_id,
      name_en: e.continent.names.en
    },
    country: {
      geoname_id: e.country.geoname_id,
      iso_code: e.country.iso_code,
      is_in_european_union: e.country.is_in_european_union,
      geoname_id: e.country.geoname_id,
      name: e.country.names.en
    },
    location: {
      accuracy_radius: e.location.accuracy_radius,
      latitude: e.location.latitude,
      longitude: e.location.longitude,
      time_zone: e.location.time_zone
    },
    postal: {
      code: e.postal.code
    },
    registered_country: {
      geoname_id: e.registered_country.geoname_id,
      is_in_european_union: e.registered_country.is_in_european_union,
      iso_code: e.registered_country.iso_code,
      name_en: e.registered_country.names.en
    },
    subdivisions: e.subdivisions.map(e => ({geoname_id: e.geoname_id, iso_code: e.iso_code, name_en: e.names.en}))
  })

const ipLookup = async xs => 
  await maxmind.open('./mmdb/GeoLite2-City.mmdb')
    .then(lookup => xs.map(e => e.is_bot 
      ? e 
      : ({...e, geo: lookup.get(e.data.ip)})))
    .then(o => o.map(e => e.is_bot 
      ? e
      : ({...e, geo: reduceIPdata(e.geo)})))
    .catch(e => console.log(e) || xs)

const constructFirehoseResponse = ps =>
  ps.then(xs => xs.map(e => e.is_bot
    ? ({recordId: e.recordId,
        result: 'Dropped',
        data: e.data
    })
    : ({
    recordId: e.recordId,
    result: 'Ok',
    data: {
    system_source: e.data.system_source, 
    api_key: e.data.api_key,
    message_id: e.data.message_id,
    trace_id: e.data.trace_id,
    received_at_apig: e.data.received_at_apig,
    ip: e.data.ip,
    user_agent: e.data.user_agent,
    ua_detected: e.ua_detected,
    body: e.data.body,
    geo: e.geo
  }})))
    .then(xs => xs.map(e => ({...e, data: b64Encode(e.data)}))) 
    .then(xs => console.log(xs) || xs)
    .then(xs => ({records: xs}))

const handler = async event =>
  pipe([
    getProp ('records'),
    getData,
    parseBody,
    parseUA,
    runBotDetector,
    markBots,
    runDeviceDetector,
    ipLookup,
    constructFirehoseResponse,
  ])(event)

module.exports.handler = handler
