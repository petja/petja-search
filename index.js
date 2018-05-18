console.log(`
*************************************
*                                   *
*        AWESOME SEARCH (TM)        *
*          BY PETJA TOURU           *
*                                   *
*************************************
`)

const emoji = require('emojify-tag')
const redis = require('redis')
const crypto = require('crypto')
const { promisifyAll } = require('bluebird')
const _ = require('lodash')

promisifyAll(redis.RedisClient.prototype)
promisifyAll(redis.Multi.prototype)

const redisClient = redis.createClient()
redisClient.on('error', console.error)

const sortQueryKeys = query =>
    Object.keys(query)
        .sort()
        .reduce(
            (acc, key) => ({
                ...acc,
                [key]: query[key],
            }),
            {}
        )

const createQueryHash = query => {
    const hash = crypto.createHash('sha256')
    const json = JSON.stringify(query)

    hash.update(json)

    return hash.digest('hex')
}

const search = async (query = {}, opts = {}, _internals = {}) => {
    const { rebuild, facets = [] } = opts
    const { withoutProps = [], write = null } = _internals

    const tempQuery = getObjWithoutSpecificKeys(query, withoutProps)
    const facetKey = { query, facets }

    // Try to find matching cache item
    const faceted = _internals.faceted || (await readCache('facets', facetKey))
    const cacheMatch = write || (await readCache('search', tempQuery))

    // If there isn't cache hit, remove properties
    // one by one to broaden search
    if (!cacheMatch) {
        // If there's no cache hit for search without any filters,
        // then we have to build the very new cache from ground up
        if (_.isEmpty(tempQuery)) {
            console.log(
                emoji`:arrows_counterclockwise: Need full refresh ...\n`
            )

            await rebuild().then(hits => {
                return writeCache('search', {}, hits)
            })

            return search(query, opts, _internals)
        }

        const lastKey = _.last(Object.keys(tempQuery))

        console.log(
            emoji`:no_entry_sign: Cache not found, trying search without "${lastKey}"\n`
        )

        return search(query, opts, {
            ..._internals,
            withoutProps: [...withoutProps, lastKey],
            write: null,
        })
    }

    const parsed =
        typeof cacheMatch === 'string' ? JSON.parse(cacheMatch) : cacheMatch

    // Start restoring removed properties if any
    if (withoutProps.length > 0) {
        const filterName = _.last(withoutProps)
        const filterValue = query[filterName]

        const hits = await runFilter(parsed, filterName, filterValue)

        const newWithout = withoutProps.slice(0, -1)
        const newQuery = getObjWithoutSpecificKeys(query, newWithout)

        const facetObj = faceted || reduceFacets(facets, hits)

        await writeCache('search', newQuery, hits)
        await writeCache('facets', facetKey, facetObj)

        return search(query, opts, {
            ..._internals,
            withoutProps: newWithout,
            write: hits,
            faceted: facetObj,
        })
    }

    return {
        hits: parsed,
        nbHits: parsed.length,
        facets: typeof faceted === 'string' ? JSON.parse(faceted) : faceted,
    }
}

const reduceFacets = (facets, hits) =>
    hits.reduce((acc, hit) => {
        return facets.reduce((acc2, facet) => {
            const fv = hit[facet]
            if (!acc2[facet]) acc2[facet] = {}

            if (Array.isArray(fv)) {
                acc2[facet] = fv.reduce((acc3, arrItem) => {
                    if (!acc3[arrItem]) acc3[arrItem] = 0
                    acc3[arrItem]++
                    return acc3
                }, acc2[facet])
            } else if (['boolean', 'number', 'string'].includes(typeof fv)) {
                if (!acc2[facet][fv]) acc2[facet][fv] = 0
                acc2[facet][fv]++
            }

            return acc2
        }, acc)
    }, {})

const runFilter = (before, filterName, filterValue) => {
    console.log(emoji`:sunglasses: Filtering`, { filterName, filterValue })

    return before.filter(row => {
        return row[filterName] === filterValue
    })
}

const getObjWithoutSpecificKeys = (obj = {}, withoutProps = []) =>
    Object.keys(obj)
        .filter(propName => !withoutProps.includes(propName))
        .reduce(
            (acc, propName) => ({
                ...acc,
                [propName]: obj[propName],
            }),
            {}
        )

const readCache = (namespace, query) => {
    const ordered = sortQueryKeys(query)
    const hash = createQueryHash(ordered)

    console.log(
        emoji`:mag: Reading cache (:key: ${hash.substr(0, 8)})\n`,
        query,
        '\n'
    )

    return redisClient.getAsync(`${namespace}:${hash}`)
}

const writeCache = async (namespace, query, hits) => {
    const ordered = sortQueryKeys(query)
    const hash = createQueryHash(ordered)

    console.log(
        emoji`:floppy_disk: Writing ${
            Object.keys(hits).length
        } rows to cache :key: ${hash.substr(0, 8)}\n`
    )

    const key = `${namespace}:${hash}`
    await redisClient.setAsync(key, JSON.stringify(hits))
    await redisClient.expireAsync(key, 900)
}

module.exports = { search }
