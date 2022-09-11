var mqtt = require('./modules/mqttconnector')
var LOG = require('./modules/LogManager')
var xml2js = require('xml2js');
var fs = require('fs');
const { time } = require('console');

module.id = "MAIN"

var artifacts = []
var stakeholders = []

var events = new Map() //artifact/stakeholder name -> [event]

var parseString = xml2js.parseString;
var config = undefined

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
//Add brokers

//Add artifacts
var artifactsbuff = config['configuration']['artifact']
for (var a in artifactsbuff) {
    artifacts[a] = {
        name: artifactsbuff[a]['name'][0],
        id: artifactsbuff[a]['id'][0],
        host: artifactsbuff[a]['host'][0],
        port: artifactsbuff[a]['port'][0],
        file: artifactsbuff[a]['stream-file-path'][0]
    }
    events.set(artifacts[a].name + '/' + artifacts[a].id, [])
}
//Add stakeholder
var stakeholdersbuff = config['configuration']['stakeholder']
for (var s in stakeholdersbuff) {
    stakeholders[s] = {
        name: stakeholdersbuff[s]['name'][0],
        process_instance: stakeholdersbuff[s]['process-instance'][0],
        host: stakeholdersbuff[s]['host'][0],
        port: stakeholdersbuff[s]['port'][0],
        file: stakeholdersbuff[s]['stream-file-path'][0]
    }
}
//Set up brokers
var brokers = config['configuration']['broker']
for (var b in brokers) {
    mqtt.createConnection(brokers[b].host[0], brokers[b].port[0], brokers[b].user[0], brokers[b].password[0], 'emulator-' + Math.random().toString(16).substr(2, 8))
}

//Open stream files and fill up events array
for (var a in artifacts) {
    const file = fs.readFileSync(artifacts[a].file, 'utf8');
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
        events.get(artifacts[a].name + '/' + artifacts[a].id).push(event)
    }
}

//Function to register an event in the future
function registerTimeout(topic, event) {
    timeouts.push(setTimeout(function () {
        var time = Math.floor(Date.now() / 1000);
        var payloadData = { timestamp: time }
        //iterate through attributes of event
        for (a in event.datanames) {
            payloadData[event.datanames[a]] = event.datas[a]
        }
        var eventstr = `{"event": {"payloadData":` + JSON.stringify(payloadData) + `}}`
        //mqtt.publishTopic(topic)
        console.log(`${topic} -> ${eventstr}`)
    }, event.time));
}


//iterate through all artifacts/stakeholders
events.forEach((value, key) => {
    //iterate through all events of the artifact
    for (var e in value) {
        //Set timeout and perform publication to the topic
        var topic = key + '/status'
        var event = value[e]
        registerTimeout(topic, event)
    }
});