var xml2js = require('xml2js');
var fs = require('fs');

var mqtt = require('./modules/egsm-common/communication/mqttconnector')
var LOG = require('./modules/egsm-common/auxiliary/logManager')


var parseString = xml2js.parseString;

var original = fs.readFileSync("./shipment-1-data/shipper1.csv", 'utf8');
var lines = original.split('\n')
var firstlineelements = lines[0].split(';')

for (var i = 2; i < 61; i++) {
    firstlineelements[firstlineelements.length - 1] = i.toString()
    var newFile = ""
    for (line in firstlineelements) {
        newFile += firstlineelements[line]
        newFile += ';'
    }
    newFile = newFile.slice(0, -1)
    newFile += '\n'
    for (var line = 1; line < lines.length; line++) {
        newFile += lines[line]
        newFile += '\n'
    }
    newFile = newFile.slice(0, -1)
    var filename = './shipment-1-data/shipper' + i.toString() + '.csv'
    fs.writeFileSync(filename,newFile, 'utf8')
}


