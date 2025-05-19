const xml2js = require('xml2js');
const fs = require('fs');
const WebSocket = require('ws');
const mqtt = require('./modules/egsm-common/communication/mqttconnector');
const LOG = require('./modules/egsm-common/auxiliary/logManager');

module.id = "MAIN";

// Global variables
let config;
let selectedInstances = [];
let entities = new Map(); // Map of entities (artifacts and stakeholders) that will emit events
let events = new Map();   // Map of events for each entity

// WebSocket connection details
const wsEndpoint = 'ws://localhost:8081';
const wsProtocol = 'data-connection';

// Parse command line arguments
function parseCommandLineArgs() {
    // Check if config file is provided
    if (process.argv.length < 3) {
        LOG.logSystem('ERROR', 'No configuration file provided', module.id);
        console.error("Usage: node emulator.js <config-file> [--instance instance1,instance2,...] [--process-type type1,type2,...]");
        process.exit(1);
    }

    let targetProcessTypes = [];
    
    for (let i = 3; i < process.argv.length; i++) {
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

    // If process types are specified, find all instances of those types
    if (targetProcessTypes.length > 0) {
        LOG.logSystem('DEBUG', `Target process types: ${targetProcessTypes.join(', ')}`, module.id);
        
        // Extract all instances of specified process types from config
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
    
    // If no specific instances or process types selected, include all instances
    if (selectedInstances.length === 0 && targetProcessTypes.length === 0) {
        const stakeholders = config.configuration.stakeholder || [];
        for (const stakeholder of stakeholders) {
            selectedInstances.push(stakeholder['process-instance'][0]);
        }
        LOG.logSystem('DEBUG', `No filters specified, selecting all instances: ${selectedInstances.join(', ')}`, module.id);
    }
    
    // Remove duplicates
    selectedInstances = [...new Set(selectedInstances)];
    LOG.logSystem('INFO', `Final selected instances: ${selectedInstances.join(', ')}`, module.id);
}

// Read and parse configuration file
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

// Setup entities (artifacts and stakeholders) based on selected instances
function setupEntities() {
    // Create a map of instance prefixes for matching artifacts
    const instancePrefixMap = new Map();
    
    // Process stakeholders first to determine paths for selected instances
    LOG.logSystem('DEBUG', 'Setting up stakeholders', module.id);
    const stakeholders = config.configuration.stakeholder || [];
    
    for (const stakeholder of stakeholders) {
        const processInstance = stakeholder['process-instance'][0];
        
        // Skip if not in selected instances
        if (!selectedInstances.includes(processInstance)) {
            continue;
        }
        
        const name = stakeholder.name[0];
        const key = `${name}/${processInstance}`;
        const path = stakeholder['stream-file-path'][0];
        
        // Create stakeholder entity
        entities.set(key, {
            type: 'stakeholder',
            name: name,
            process_instance: processInstance,
            host: stakeholder.host[0],
            port: stakeholder.port[0],
            file: path
        });
        
        events.set(key, []);
        
        // Extract prefix for matching artifacts
        const pathParts = path.split('/');
        if (pathParts.length >= 3) {
            // Extract prefix from path (e.g., shipment-1-data/AMS-CDG/06-AMS-CDG-)
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
    
    // Process artifacts and match them to instances based on path prefix
    LOG.logSystem('DEBUG', 'Setting up artifacts', module.id);
    const artifacts = config.configuration.artifact || [];
    
    for (const artifact of artifacts) {
        const name = artifact.name[0];
        const id = artifact.id[0];
        const path = artifact['stream-file-path'][0];
        
        // Find matching instance for this artifact
        let matchedInstance = null;
        
        for (const [instance, prefix] of instancePrefixMap.entries()) {
            if (path.startsWith(prefix)) {
                matchedInstance = instance;
                break;
            }
        }
        
        // Skip if no matching instance found or instance not selected
        if (!matchedInstance || !selectedInstances.includes(matchedInstance)) {
            continue;
        }
        
        const key = `${name}/${id}`;
        
        // Create artifact entity
        entities.set(key, {
            type: 'artifact',
            name: name,
            id: id,
            host: artifact.host[0],
            port: artifact.port[0],
            file: path,
            process_instance: matchedInstance  // Track associated instance
        });
        
        events.set(key, []);
    }
    
    LOG.logSystem('INFO', `Setup ${entities.size} entities for emulation`, module.id);
}

// Setup MQTT brokers from config
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

// Read stream files and organize events
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

// Register a timeout for an event
function registerTimeout(entityName, event) {
    setTimeout(() => {
        const entity = entities.get(entityName);
        const topic = entity.type === 'artifact' ? `${entityName}/status` : entityName;
        const payloadData = {};
        
        if (entity.type === 'artifact') {
            payloadData.timestamp = Math.floor(Date.now() / 1000);
        }
        
        // Add event data to payload
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

// Setup events for emulation
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

// Create process instances via WebSocket
async function createProcessInstances() {
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(wsEndpoint, wsProtocol);
        
        ws.onopen = () => {
            LOG.logSystem('DEBUG', `WebSocket connection opened to ${wsEndpoint}`, module.id);
            let createdInstances = 0;
            
            // Find process type for each instance from stakeholder paths
            const instanceTypeMap = new Map();
            
            for (const entity of entities.values()) {
                if (entity.type === 'stakeholder') {
                    const pathParts = entity.file.split('/');
                    if (pathParts.length >= 2) {
                        instanceTypeMap.set(entity.process_instance, pathParts[1]);
                    }
                }
            }
            
            const promises = selectedInstances.map(instance => {
                return new Promise((resolveInstance, rejectInstance) => {
                    const processType = instanceTypeMap.get(instance);
                    
                    if (!processType) {
                        LOG.logSystem('WARNING', `Could not determine process type for instance ${instance}`, module.id);
                        resolveInstance();
                        return;
                    }
                    
                    const message = JSON.stringify({
                        type: "command",
                        module: "new_process_instance",
                        payload: {
                            instance_name: instance,
                            bpmn_job_start: true,
                            process_type: processType
                        }
                    });
                    
                    ws.send(JSON.stringify(message), err => {
                        if (err) {
                            LOG.logSystem('ERROR', `Failed to send create command for instance ${instance}: ${err}`, module.id);
                            rejectInstance(err);
                        } else {
                            LOG.logSystem('DEBUG', `Created process instance ${instance} of type ${processType}`, module.id);
                            createdInstances++;
                            resolveInstance();
                        }
                    });
                });
            });
            
            Promise.all(promises)
                .then(() => {
                    LOG.logSystem('INFO', `Successfully created ${createdInstances} process instances`, module.id);
                    ws.close();
                    resolve();
                })
                .catch(err => {
                    LOG.logSystem('ERROR', `Error creating process instances: ${err}`, module.id);
                    ws.close();
                    reject(err);
                });
        };
        
        ws.onerror = err => {
            LOG.logSystem('ERROR', `WebSocket connection error: ${err}`, module.id);
            reject(err);
        };
        
        ws.onclose = () => {
            LOG.logSystem('DEBUG', 'WebSocket connection closed', module.id);
        };
    });
}

function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Main function
async function main() {
    try {
        // Load configuration
        loadConfig();
        
        // Parse command line arguments
        parseCommandLineArgs();
        
        // Setup entities based on selected instances
        setupEntities();
        
        // Create process instances
        await createProcessInstances();
        
        // Setup MQTT brokers
        setupBrokers();
        
        // Read stream files
        readStreamFiles();
        
        // Schedule events
        setupEvents();
        
        LOG.logSystem('INFO', 'Emulation setup complete', module.id);
    } catch (err) {
        LOG.logSystem('ERROR', `Error in main execution: ${err}`, module.id);
    }
}

// Start execution
main();