var fs = require('fs');
var util = require('util');

const LOG_LEVELS = {'DEBUG': 1, 'WARNING': 2, 'ERROR': 3};
var CONSOLE_LOG_LEVEL = LOG_LEVELS.DEBUG; //Console log from a defined level above

var prefix = (new Date().toISOString().replace(/:/g,'')).replace(/\./g,'');
fs.mkdirSync('log/'+ prefix);

var system_log = fs.createWriteStream('log/'+ prefix +'/worker.log', {flags : 'w'});

var logSystem = function(type, value, location)
{
  location = location || ''
  system_log.write(new Date().toISOString() + '; ' + util.format('[' + location + '] ' + type + ' - ' + value) + '\n');
  if(LOG_LEVELS[type] >= CONSOLE_LOG_LEVEL){
    console.log(util.format('[' + location + '] ' + type + ' - ' + value))
  }
}

// exposed functions
module.exports = {
    logSystem: logSystem
}
