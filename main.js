const xml2js = require('xml2js');
const fs = require('fs');
const mqtt = require('./modules/egsm-common/communication/mqttconnector');
const LOG = require('./modules/egsm-common/auxiliary/logManager');

module.id = "MAIN";

//Global variables
let config;
let configFilePath = ".\shipment-1-data\config.xml";
let selectedInstances = [];
let entities = new Map(); //Map of entities (artifacts and stakeholders) that will emit events
let events = new Map();   //Map of events for each entity

//Parse command line arguments
function parseCommandLineArgs() {
    let targetProcessTypes = [];
    for (let i = 2; i < process.argv.length; i++) {
        if (process.argv[i] === '--process-type') {
            if (selectedInstances.length > 0) {
                LOG.logSystem('ERROR', 'Cannot use --process-type and --instance together', module.id);
                process.exit(1);
            }

            if (process.argv[i + 1] && !process.argv[i + 1].startsWith('--')) {
                targetProcessTypes = process.argv[i + 1].split(',');
                i++;
            }
        } else if (process.argv[i] === '--instance') {
            if (targetProcessTypes.length > 0) {
                LOG.logSystem('ERROR', 'Cannot use --process-type and --instance together', module.id);
                process.exit(1);
            }

            if (process.argv[i + 1] && !process.argv[i + 1].startsWith('--')) {
                selectedInstances = process.argv[i + 1].split(',');
                i++;
            }
        }
    }
    if (targetProcessTypes.length > 0) {
        LOG.logSystem('DEBUG', `Target process types: ${targetProcessTypes.join(', ')}`, module.id);
        const stakeholders = config.configuration.stakeholder || [];
        for (const stakeholder of stakeholders) {
            const path = stakeholder['stream-file-path'][0];
            const parts = path.split('/');
            if (parts.length >= 2) {
                const processType = parts[1];
                if (targetProcessTypes.includes(processType)) {
                    selectedInstances.push(stakeholder['process-instance'][0]);
                }
            }
        }
        LOG.logSystem('DEBUG', `Selected instances from process types: ${selectedInstances.join(', ')}`, module.id);
    }
    if (selectedInstances.length === 0 && targetProcessTypes.length === 0) {
        const stakeholders = config.configuration.stakeholder || [];
        for (const stakeholder of stakeholders) {
            selectedInstances.push(stakeholder['process-instance'][0]);
        }
        LOG.logSystem('DEBUG', `No filters specified, selecting all instances: ${selectedInstances.join(', ')}`, module.id);
    }
    selectedInstances = [...new Set(selectedInstances)];
    LOG.logSystem('INFO', `Final selected instances: ${selectedInstances.join(', ')}`, module.id);
}

//Read and parse configuration file
function loadConfig() {
    try {
        const data = fs.readFileSync(process.argv[2], 'utf8');
        LOG.logSystem('DEBUG', 'Configuration file read successfully', module.id);

        xml2js.parseString(data, (err, result) => {
            if (err) {
                LOG.logSystem('ERROR', `Error parsing configuration file: ${err}`, module.id);
                process.exit(1);
            }
            config = result;
        });
    } catch (err) {
        LOG.logSystem('ERROR', `Error reading configuration file: ${err}`, module.id);
        process.exit(1);
    }
}

//Setup entities (artifacts and stakeholders) based on selected instances
function setupEntities() {
    const instancePrefixMap = new Map();
    LOG.logSystem('DEBUG', 'Setting up stakeholders', module.id);
    const stakeholders = config.configuration.stakeholder || [];
    for (const stakeholder of stakeholders) {
        const processInstance = stakeholder['process-instance'][0];
        if (!selectedInstances.includes(processInstance))
            continue;
        const name = stakeholder.name[0];
        const key = `${name}/${processInstance}`;
        const path = stakeholder['stream-file-path'][0];
        entities.set(key, {
            type: 'stakeholder',
            name: name,
            process_instance: processInstance,
            host: stakeholder.host[0],
            port: stakeholder.port[0],
            file: path
        });
        events.set(key, []);
        const pathParts = path.split('/');
        if (pathParts.length >= 3) {
            //Extract prefix from path (e.g., shipment-1-data/AMS-CDG/06-AMS-CDG-)
            const dirPath = pathParts.slice(0, pathParts.length - 1).join('/');
            const fileName = pathParts[pathParts.length - 1];
            const match = fileName.match(/\d+-(.+)-[^/]+\.csv$/);
            if (match) {
                const prefix = `${dirPath}/${match[0].split('-')[0]}-${match[1]}-`;
                instancePrefixMap.set(processInstance, prefix);
                LOG.logSystem('DEBUG', `Mapped instance ${processInstance} to prefix ${prefix}`, module.id);
            }
        }
    }
    LOG.logSystem('DEBUG', 'Setting up artifacts', module.id);
    const artifacts = config.configuration.artifact || [];
    for (const artifact of artifacts) {
        const name = artifact.name[0];
        const id = artifact.id[0];
        const path = artifact['stream-file-path'][0];
        let matchedInstance = null;
        for (const [instance, prefix] of instancePrefixMap.entries()) {
            if (path.startsWith(prefix)) {
                matchedInstance = instance;
                break;
            }
        }
        if (!matchedInstance || !selectedInstances.includes(matchedInstance))
            continue;
        const key = `${name}/${id}`;
        entities.set(key, {
            type: 'artifact',
            name: name,
            id: id,
            host: artifact.host[0],
            port: artifact.port[0],
            file: path,
            process_instance: matchedInstance
        });
        events.set(key, []);
    }
    LOG.logSystem('INFO', `Setup ${entities.size} entities for emulation`, module.id);
}

//Setup MQTT brokers from config
function setupBrokers() {
    LOG.logSystem('DEBUG', 'Setting up MQTT brokers', module.id);
    const brokers = config.configuration.broker || [];
    for (const broker of brokers) {
        mqtt.createConnection(
            broker.host[0],
            broker.port[0],
            broker.user[0],
            broker.password[0],
            'emulator-' + Math.random().toString(16).substring(2, 8)
        );
    }
    LOG.logSystem('INFO', `Setup ${brokers.length} MQTT brokers`, module.id);
}

//Read stream files and organize events
function readStreamFiles() {
    LOG.logSystem('DEBUG', 'Reading stream files', module.id);
    entities.forEach((entity, key) => {
        try {
            const file = fs.readFileSync(entity.file, 'utf8').replace(/\r\n/g, '\n');
            const lines = file.split('\n');
            for (const line of lines) {
                if (!line || line.trim() === '') continue;
                const elements = line.split(';');
                const time = parseInt(elements[0]);
                if (isNaN(time)) {
                    LOG.logSystem('WARNING', `Invalid time format in file ${entity.file}: ${line}`, module.id);
                    continue;
                }
                const dataNames = [];
                const dataValues = [];
                for (let i = 1; i < elements.length; i++) {
                    if (i % 2 === 1) {
                        dataNames.push(elements[i]);
                    } else {
                        dataValues.push(elements[i]);
                    }
                }
                events.get(key).push({
                    time: time,
                    datanames: dataNames,
                    datas: dataValues
                });
            }
            LOG.logSystem('DEBUG', `Loaded ${events.get(key).length} events for ${key}`, module.id);
        } catch (err) {
            LOG.logSystem('ERROR', `Error reading stream file ${entity.file}: ${err}`, module.id);
        }
    });
}

//Register a timeout for an event
function registerTimeout(entityName, event) {
    setTimeout(() => {
        const entity = entities.get(entityName);
        const topic = entity.type === 'artifact' ? `${entityName}/status` : entityName;
        const payloadData = {};
        if (entity.type === 'artifact') {
            payloadData.timestamp = Math.floor(Date.now() / 1000);
        }
        for (let i = 0; i < event.datanames.length; i++) {
            payloadData[event.datanames[i]] = event.datas[i];
        }
        const eventStr = JSON.stringify({
            event: { payloadData: payloadData }
        });
        mqtt.publishTopic(entity.host, entity.port, topic, eventStr);
        LOG.logSystem('DEBUG', `Emitted event: [${topic}] -> [${eventStr}]`, module.id);
    }, event.time);
}

//Setup events for emulation
function setupEvents() {
    LOG.logSystem('DEBUG', 'Setting up events for emulation', module.id);
    let totalEvents = 0;
    events.forEach((eventList, entityName) => {
        eventList.forEach(event => {
            registerTimeout(entityName, event);
            totalEvents++;
        });
    });
    LOG.logSystem('INFO', `Scheduled ${totalEvents} events for emulation`, module.id);
}

async function main() {
    try {
        loadConfig();
        parseCommandLineArgs();
        setupEntities();
        setupBrokers();
        readStreamFiles();
        setupEvents();
        LOG.logSystem('INFO', 'Emulation setup complete', module.id);
    } catch (err) {
        LOG.logSystem('ERROR', `Error in main execution: ${err}`, module.id);
    }
}

main();