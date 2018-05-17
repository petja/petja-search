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

const search = async (query = {}, internals, rebuildCache) => {
    const { withoutProps = [], write = null } = internals
    const tempQuery = getObjWithoutSpecificKeys(query, withoutProps)

    // Try to find matching cache item
    const cacheMatch = write ? write : await getMatch(tempQuery)

    // If there isn't cache hit, remove properties
    // one by one to broaden search
    if (!cacheMatch) {
        // If there's no cache hit for search without any filters,
        // then we have to build the very new cache from ground up
        if (_.isEmpty(tempQuery)) {
            console.log(
                emoji`:arrows_counterclockwise: Need full refresh ...\n`
            )

            await rebuildCache().then(hits => {
                return writeCache({}, hits)
            })

            return search(query, internals, rebuildCache)
        }

        const lastKey = _.last(Object.keys(tempQuery))

        console.log(
            emoji`:no_entry_sign: Cache not found, trying search without "${lastKey}"\n`
        )

        return search(
            query,
            {
                ...internals,
                withoutProps: [...withoutProps, lastKey],
                write: null,
            },
            rebuildCache
        )
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

        await writeCache(newQuery, hits)

        return search(
            query,
            { ...internals, withoutProps: newWithout, write: hits },
            rebuildCache
        )
    }

    return parsed
}

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

/*const getCacheSteps = query => {
    const allKeys = Object.keys(query).sort()

    return allKeys.map((item, index) =>
        allKeys.slice(0, index + 1).reduce(
            (acc, key) => ({
                ...acc,
                [key]: query[key],
            }),
            {}
        )
    )
}*/

const getMatch = query => {
    const ordered = sortQueryKeys(query)
    const hash = createQueryHash(ordered)

    console.log(
        emoji`:mag: Reading cache (:key: ${hash.substr(0, 8)})\n`,
        query,
        '\n'
    )

    return redisClient.getAsync(`cached_search:${hash}`)
}

const writeCache = async (query, hits) => {
    const ordered = sortQueryKeys(query)
    const hash = createQueryHash(ordered)

    console.log(
        emoji`:pencil2: Writing ${
            hits.length
        } rows to cache :key: ${hash.substr(0, 8)}\n`
    )

    const key = `cached_search:${hash}`
    await redisClient.setAsync(key, JSON.stringify(hits))
    await redisClient.expireAsync(key, 60)
}

module.exports = { search }
