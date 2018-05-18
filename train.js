const fetch = require('node-fetch')

const toJSON = resp => resp.json()
const getYYYYMMDD = d => d.toISOString().substr(0, 10)

const _ = require('lodash')

const redis = require('redis')
const { promisifyAll } = require('bluebird')
promisifyAll(redis.RedisClient.prototype)
promisifyAll(redis.Multi.prototype)

const redisClient = redis.createClient()
redisClient.on('error', console.error)

const PetjaSearch = require('./index')(redisClient)

const fetchTrains = (d = new Date()) =>
    fetch(`https://rata.digitraffic.fi/api/v1/trains/${getYYYYMMDD(d)}`).then(
        async resp => {
            const txt = await resp.text()
            try {
                return JSON.parse(txt)
            } catch (err) {
                console.log(err, txt.substr(0, 300))
            }
        }
    )

const dayIterator = (dateStart, days = 0, now = 0, arr = []) => {
    const date = new Date(dateStart)
    date.setDate(date.getDate() + now)

    if (now < days) {
        return dayIterator(dateStart, days, now + 1, [...arr, date])
    }

    return arr
}

const pushTrains = async () => {
    const daysBefore = 7
    const daysAfter = 7
    const daysAll = daysBefore + daysAfter + 1

    const dateStart = new Date()
    dateStart.setDate(dateStart.getDate() - daysBefore)
    dateStart.setHours(0, 0, 0, 0)

    const beforeFetch = Date.now()

    console.log('Fetching dates...')

    const dates = dayIterator(dateStart, daysAll).map(date => () =>
        fetchTrains(date).then(trains => {
            console.log(
                `Trains of the date ${getYYYYMMDD(date)} have been fetched`
            )
            return trains
        })
    )

    const sequential = require('promise-sequential')
    const allDates = await sequential(dates)

    const trains = _.flatten(allDates)

    const seconds = (Date.now() - beforeFetch) / 1000

    console.log(
        `All days have been fetched!\nFound ${
            trains.length
        } trains in ${seconds.toFixed(1)} seconds.`
    )

    return trains.map(train => {
        const {
            departureDate,
            trainNumber,
            trainType,
            timeTableRows,
            timetableType,
            cancelled,
            commuterLineID,
            operatorShortCode,
        } = train

        const initialState = {
            viaStations: [],
        }

        const additionalInfo = timeTableRows.reduce((acc2, row) => {
            const { stationShortCode } = row

            if (!acc2.viaStations.includes(stationShortCode)) {
                acc2.viaStations.push(stationShortCode)
            }

            return acc2
        }, initialState)

        const fromStation = timeTableRows[0].stationShortCode
        const toStation =
            timeTableRows[timeTableRows.length - 1].stationShortCode

        return {
            objectID: [departureDate, trainNumber].join('/'),
            departureDate,
            timetableType,
            trainNumber,
            trainType,
            fromStation,
            toStation,
            cancelled,
            commuterLineID,
            operatorShortCode,
            ...additionalInfo,
        }
    })
}

const args = process.argv.splice(process.execArgv.length + 2)
const query = JSON.parse(args[0].trim())

PetjaSearch(query, {
    rebuild: pushTrains,
    facets: ['commuterLineID', 'trainType', 'viaStations'],
})
    .then(({ nbHits, facets }) => {
        console.log(
            require('util').inspect(
                { nbHits, facets },
                { colors: true, depth: 2 }
            )
        )
    })
    .catch(console.error)
