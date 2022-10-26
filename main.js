var xml2js = require('xml2js');
var fs = require('fs');

var mqtt = require('./modules/egsm-common/communication/mqttconnector')
var LOG = require('./modules/egsm-common/auxiliary/logManager')

module.id = "MAIN"

//Global variables
var entities = new Map() //Defined entites in the config file which are capable to event emitting {artifact or stakeholder name} -> {details}
var events = new Map() //Future events defined in the stream files (each event corresponds one line){artifact or stakeholder name} -> [{event}]
var config = undefined //System config

var parseString = xml2js.parseString;

//Reading config file
try {
    const data = fs.readFileSync(process.argv[2], 'utf8');
    LOG.logSystem('DEBUG', 'Initialization file read', module.id)

    parseString(data, function (err, result) {
        if (err) {
            LOG.logSystem('ERROR', `Error while parsing initialization file: ${err}`, module.id)
            throw 'parsing_error'
        }
        config = result
    })
} catch (err) {
    LOG.logSystem('DEBUG', `Error while reading initialization file: ${err}`, module.id)
    return
}

//Add artifacts
LOG.logSystem('DEBUG', 'Adding artifacts', module.id)
var artifactsbuff = config['configuration']['artifact']
for (var a in artifactsbuff) {
    var key = artifactsbuff[a]['name'][0] + '/' + artifactsbuff[a]['id'][0]
    if (entities.has(key)) {
        LOG.logSystem('ERROR', 'Same Artifact Name and ID used twice', module.id)
        throw 'Same Artifact Name and ID used twice'
    }
    entities.set(key, {
        type: 'artifact',
        name: artifactsbuff[a]['name'][0],
        id: artifactsbuff[a]['id'][0],
        host: artifactsbuff[a]['host'][0],
        port: artifactsbuff[a]['port'][0],
        file: artifactsbuff[a]['stream-file-path'][0]
    })
    events.set(key, [])
}

//Add stakeholder
LOG.logSystem('DEBUG', 'Adding stakeholders', module.id)
var stakeholdersbuff = config['configuration']['stakeholder']
for (var s in stakeholdersbuff) {
    var key = stakeholdersbuff[s]['name'][0] + '/' + stakeholdersbuff[s]['process-instance'][0]
    if (entities.has(key)) {
        LOG.logSystem('ERROR', 'Same Stakeholder Name and ID used twice', module.id)
        throw 'Same Stakeholder Name and ID used twice'
    }
    entities.set(key, {
        type: 'stakeholder',
        name: stakeholdersbuff[s]['name'][0],
        process_instance: stakeholdersbuff[s]['process-instance'][0],
        host: stakeholdersbuff[s]['host'][0],
        port: stakeholdersbuff[s]['port'][0],
        file: stakeholdersbuff[s]['stream-file-path'][0]
    })
    events.set(key, [])
}

//Set up brokers
LOG.logSystem('DEBUG', 'Adding brokers', module.id)
var brokers = config['configuration']['broker']
for (var b in brokers) {
    mqtt.createConnection(brokers[b].host[0], brokers[b].port[0], brokers[b].user[0], brokers[b].password[0], 'emulator-' + Math.random().toString(16).substr(2, 8))
}

//Open stream files and fill up events array
LOG.logSystem('DEBUG', 'Reading stream files and organizing events', module.id)
entities.forEach((value, key) => {
    var file = fs.readFileSync(value.file, 'utf8');
    file = file.replaceAll('\r\n', '\n')
    var lines = file.split('\n')
    for (var l in lines) {
        var lineelements = lines[l].split(';')
        var datanames = []
        var datas = []
        for (var i = 1; i < lineelements.length; i++) {
            if (i % 2 == 0) {
                datas.push(lineelements[i])
            }
            else {
                datanames.push(lineelements[i])
            }
        }
        var event = {
            time: lineelements[0],
            datanames: datanames,
            datas: datas
        }
        events.get(key).push(event)
    }
})

//Function to register an event in the future
function registerTimeout(entityname, event) {
    setTimeout(function () {
        var topic = entityname
        var payloadData = {}
        if (entities.get(entityname).type == 'artifact') {
            topic += '/status'
            payloadData['timestamp'] = Math.floor(Date.now() / 1000)
        }
        //iterate through attributes of event
        for (a in event.datanames) {
            payloadData[event.datanames[a]] = event.datas[a]
        }
        var eventstr = `{"event": {"payloadData":` + JSON.stringify(payloadData) + `}}`
        mqtt.publishTopic(entities.get(entityname).host, entities.get(entityname).port, topic, eventstr)
        LOG.logSystem('DEBUG', `Emitting event: [${topic}] -> [${eventstr}]`, module.id)
    }, event.time);
}


//iterate through all artifacts/stakeholders
LOG.logSystem('DEBUG', 'Creating future events', module.id)
events.forEach((value, key) => {
    //iterate through all events of the artifact
    for (var e in value) {
        //Set timeout and perform publication to the topic
        registerTimeout(key, value[e])
    }
});