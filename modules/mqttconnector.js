var mqtt = require("mqtt")
var LOG = require('./LogManager')

module.id = "MQTT"

function MqttBroker(hostname, port, userName, userPassword, clientId) {
    var opts = { clean: true, host: hostname, port: port, username: userName, password: userPassword, keepalive: 30, clientId: clientId, protocolVersion: 5 };
    return {
        mqttclient: mqtt.connect(opts)
            .on('connect', function () {
                LOG.logSystem('DEBUG', `Connected to broker: ${hostname}:${port}`, module.id)
            })
            .on('disconnect', function () {
                LOG.logSystem('DEBUG', `Disconnected from broker: ${hostname}:${port}`, module.id)
            })
            .on('reconnect', function () {
                LOG.logSystem('DEBUG', `Reconnected to broker: ${hostname}:${port}`, module.id)
            })
            .on('error', function (error) {
                LOG.logSystem('DEBUG', `Broker error [${error}] at Broker: ${hostname}:${port}`, module.id)
            })
            .on('message', function (topic, message) {
                LOG.logSystem('DEBUG', `New message: [${hostname}:${port}]::[${topic}]->[${message}]`, module.id)
                ON_RECEIVED(hostname, port, topic, message)
            })
    }
}

let BROKERS = new Map(); // {IP, PORT} -> BROKER_DETAILS
var ON_RECEIVED = undefined; //Function reference called when a new message available from a topic

module.exports = {

    init: function (onReceivedFunction) {
        LOG.logSystem('DEBUG', `init called`, module.id)
        ON_RECEIVED = onReceivedFunction
    },

    createConnection: function (hostname, port, username, userpassword, clientid) {
        LOG.logSystem('DEBUG', `createConnection called: ${hostname}:${port}`, module.id)
        if (!BROKERS.has([hostname, port].join(":"))) {
            var newBroker = new MqttBroker(hostname, port, username, userpassword, clientid)
            BROKERS.set([hostname, port].join(":"), newBroker)
            LOG.logSystem('DEBUG', `Connection created: ${hostname}:${port}`, module.id)
            return 'created'
        }
        LOG.logSystem('DEBUG', `Connection is already existing: ${hostname}:${port}`, module.id)
        return 'connection_exists'
    },

    closeConnection: function (hostname, port) {
        LOG.logSystem('DEBUG', `closeConnection called: ${hostname}:${port}`, module.id)
        if (BROKERS.has([hostname, port].join(":"))) {
            BROKERS.get([hostname, port].join(":")).mqttclient.end()
            BROKERS.delete([hostname, port].join(":"))
            LOG.logSystem('DEBUG', `Connection closed: ${hostname}:${port}`, module.id)
        }
    },

    publishTopic: function (hostname, port, topic, message) {
        LOG.logSystem('DEBUG', `Publishing to: [${hostname}]:[${port}] -> [${topic}] :: [${message}]`, module.id)
        if (!BROKERS.has([hostname, port].join(":"))) {
            LOG.logSystem('WARNING', `Specified Broker is not defined: [${hostname}]:[${port}]`, module.id)
            return
        }
        BROKERS.get([hostname, port].join(":")).mqttclient.publish(topic, message);
    },

    subscribeTopic: function (hostname, port, topic) {
        LOG.logSystem('DEBUG', `Subscribing to: [${hostname}]:[${port}] -> [${topic}]`, module.id)
        if (!ON_RECEIVED) {
            LOG.logSystem('WARNING', `ON_RECEIVED function not defined yet, use init() function!`, module.id)
        }
        if (!BROKERS.has([hostname, port].join(":"))) {
            LOG.logSystem('WARNING', `Specified Broker is not defined: [${hostname}]:[${port}]`, module.id)
            return
        }
        BROKERS.get([hostname, port].join(":")).mqttclient.subscribe(topic, { nl: true });
    },

    unsubscribeTopic: function (hostname, port, topic) {
        LOG.logSystem('DEBUG', `Unsubscribing from: [${hostname}]:[${port}] -> [${topic}]`, module.id)
        if (!BROKERS.has([hostname, port].join(":"))) {
            LOG.logSystem('WARNING', `Specified Broker is not defined: [${hostname}]:[${port}]`, module.id)
            return
        }
        BROKERS.get([hostname, port].join(":")).mqttclient.unsubscribe(topic)
    },
};