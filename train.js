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
            objectID: trainNumber,
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

const query = {
    cancelled: true,
    trainType: 'T',
}

search(query, {}, pushTrains)
    .then(items => {
        console.log(`\n${items.length} results`)
        //console.log(items.slice(0, 3))
    })
    .catch(console.error)
