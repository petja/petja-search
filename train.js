const fetch = require('node-fetch')

const toJSON = resp => resp.json()
const getYYYYMMDD = d => d.toISOString().substr(0, 10)

const fetchTrains = (d = new Date()) =>
    fetch(`https://rata.digitraffic.fi/api/v1/trains/${getYYYYMMDD(d)}`).then(
        toJSON
    )

const { search } = require('./index')

const pushTrains = async () => {
    console.log('Penetrating to Digitraffic API', { timestamp: new Date() })
    const trains = await fetchTrains()
    console.log('Penetration complete', { timestamp: new Date() })

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

search(query, {
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
