var util = require('util');
var mqtt = require('mqtt');

const commandLineArgs = require('command-line-args')
var errorCounter = 0;
var gridBalance = 0;
var PVPower = {};
var batteryPower = {};
var EVSEPower = {};
var dimmablePower = {};
var activePower = {};
var PVEnergy = {};
var todayPVEnergy = {};
var startup = Date.now();
var nrg = {};
var totalLoadPower = 0;
var gridBalanceAge = 0;
var state = {};

const optionDefinitions = [
  { name: 'mqtthost', alias: 'm', type: String, defaultValue: "localhost" },
  { name: 'mqttclientid', alias: 'M', type: String, defaultValue: "mqtt2agg" },
  { name: 'inverter', alias: 'i', type: String, multiple: true, defaultValue: ['Huawei/#', 'GoodWe/#', 'SMA/#', 'Hoymiles/#', 'Kostal/#', 'SUNSPEC/#'] },

  { name: 'gridmeter', alias: 'g', type: String },
  { name: 'gridmeterfield', alias: 'f', type: String, defaultValue: "Power"},
  { name: 'evse', alias: 'e', type: String, multiple: true , defaultValue: []},
  { name: 'evsefield', type: String, multiple: true, defaultValue: ['TotalActivePower', 'TotalActivePower'] },
  { name: 'dimmable', alias: 'D', type: String, multiple: true, defaultValue: ['SM-DRT/HS'] },
  { name: 'dimmablefield', alias: 'F', type: String, multiple: true, defaultValue: ['TotalActivePower'] },
  { name: 'wait', alias: 'w', type: Number, defaultValue: 15000 },
  { name: 'debug', alias: 'd', type: Boolean, defaultValue: false },
  { name: 'goecharger', alias: 'E', type: String, multiple: true, defaultValue: ['+'] }
];

const options = commandLineArgs(optionDefinitions)

console.log("MQTT Host         : " + options.mqtthost);
console.log("MQTT Client ID    : " + options.mqttclientid);
console.log("Inverters         : " + options.inverter);
console.log("Grid meter        : " + options.gridmeter);
console.log("Grid meter field  : " + options.gridmeterfield);
console.log("Go-eChargers      : go-eCharger/" + options.goecharger);

var MQTTclient = mqtt.connect("mqtt://" + options.mqtthost);
MQTTclient.on("connect", function () {
  console.log("MQTT connected");
})

MQTTclient.on("error", function (error) {
  console.log("Can't connect" + error);
  process.exit(1)
});

function sendMqtt(topic, data) {
  if (options.debug) {
    console.log("publish: " + topic, JSON.stringify(data));
  }
  MQTTclient.publish(topic, JSON.stringify(data), { retain: true })
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

for (let address of options.goecharger) {
  if (options.debug) {
    console.log("subscribe: go-eCharger/" + address + "/...");
  }
  MQTTclient.subscribe("go-eCharger/" + address + "/nrg");
  MQTTclient.subscribe("go-eCharger/" + address + "/eto");
}

for (let address of options.inverter) {
  if (options.debug) {
    console.log("subscribe: " + address);
  }
  MQTTclient.subscribe(address);
}

for (let address of options.evse) {
  if (options.debug) {
    console.log("subscribe: " + address);
  }
  MQTTclient.subscribe(address);
}

for (let address of options.dimmable) {
  if (options.debug) {
    console.log("subscribe: " + address);
  }
  MQTTclient.subscribe(address);
}

MQTTclient.subscribe(options.gridmeter);

async function roundValues(object, fixed) {
  for (const [key, value] of Object.entries(object)) {
    if(isNaN(value)) {
      object[key] = 0;
    } else {
      object[key] = parseFloat(value.toFixed(fixed));
    }
  }
}


function sendAggregates() {
  if ((Date.now() - startup) > options.wait) {

    state.totalPVPower = 0;
    state.totalEVSEPower = 0;
    state.totalDimmablePower = 0;
    state.totalActivePower = 0;
    state.totalBatteryPower = 0;
    state.load = 0;
    state.totalPVEnergy = 0;
    state.dayPVEnergy = 0;
    state.gridBalance = gridBalance;

    for (const [key, value] of Object.entries(PVPower)) {
      state.totalPVPower += value;
    }
    for (const [key, value] of Object.entries(EVSEPower)) {
      state.totalEVSEPower += value;
    }
    for (const [key, value] of Object.entries(dimmablePower)) {
      state.totalDimmablePower += value;
    }
    for (const [key, value] of Object.entries(activePower)) {
      state.totalActivePower += value;
    }
    for (const [key, value] of Object.entries(PVEnergy)) {
      state.totalPVEnergy += value;
    }
    for (const [key, value] of Object.entries(todayPVEnergy)) {
      state.dayPVEnergy += value;
    }
    for (const [key, value] of Object.entries(batteryPower)) {
      state.totalBatteryPower += value;
    }
    state.load = state.totalPVPower + state.gridBalance + state.totalBatteryPower;
    roundValues(state, 3);
    if (options.debug) {
      console.log("totalPVEnergy:", state.totalPVEnergy, "dayPVEnergy:", state.dayPVEnergy, " gridBalance: ", state.gridBalance, " BatteryPower: ", state.totalBatteryPower, " Load: ", state.load, " totalActivePower:", state.totalActivePower, " totalPVPower:", state.totalPVPower, " totalEVSEPower:", state.totalEVSEPower);
    }
    sendMqtt("agg/" + options.mqttclientid, state);
  }
}

function findVal(object, key) {
  var value;
  Object.keys(object).some(function (k) {
    if (k === key) {
      value = object[k];
      return true;
    }
    if (object[k] && typeof object[k] === 'object') {
      value = findVal(object[k], key);
      return value !== undefined;
    }
  });
  return value;
}

MQTTclient.on('message', function (topic, message, packet) {
  //  console.log(topic + message);
  if (topic.includes("go-eCharger/")) {
    let sub = topic.split('/');
    let id = sub[1];
    let func = sub[2];
    let obj = JSON.parse(message);
    let index;
    
    if(obj) {
      if (func == 'nrg') {
        if (obj.length > 15) {
          nrg.UL1 = obj[0];
          nrg.UL2 = obj[1];
          nrg.UL3 = obj[2];
          nrg.UN = obj[3];
          nrg.IL1 = obj[4];
          nrg.IL2 = obj[5];
          nrg.IL3 = obj[6];
          nrg.PL1 = obj[7];
          nrg.PL2 = obj[8];
          nrg.PL3 = obj[9];
          nrg.PN = obj[10];
          nrg.P = obj[11];
          nrg.pfL1 = obj[12];
          nrg.pfL2 = obj[13];
          nrg.pfL3 = obj[14];
          nrg.pfN = obj[15];
          if (options.debug) {
            console.log(util.inspect(nrg));
          }
          if(options.evse.length == 0) {
            if(options.debug) {
              console.log("EVSEPower: " + id + " Power: " + nrg.P);
            }
            EVSEPower[id] = nrg.P;
          }
          sendMqtt("go-eCharger/" + id + "/agg", nrg);
          if(findVal(state, "gridBalance") && (Date.now()-gridBalanceAge) < 10000) {
            var goEgrid = { "pGrid":state.gridBalance, "pPv":state.totalPVPower, "pAkku":state.totalBatteryPower};
            if(options.debug) {
              console.log("go-eCharger: ids ", id, goEgrid);
            }
            sendMqtt("go-eCharger/"+id+"/ids/set", goEgrid);
          }
        }
      } else if (func == 'eto') {
        nrg.eto = obj;
        if (options.debug) {
          console.log(util.inspect(nrg));
        }
        sendMqtt("go-eCharger/" + id + "/agg", nrg);
      }
    }
  } else if (topic.includes("Huawei/") || topic.includes("GoodWe/") || topic.includes("Kostal/") || topic.includes("SMA/") || topic.includes("SUNSPEC/")) {
    let id = topic.split('/')[1];
    let obj = JSON.parse(message);
    if(options.debug) {
      console.log(id + util.inspect(obj));
    }
    var val = 0;
    PVPower[id] = 0;
    
    val = findVal(obj, 'MPPT1Power');
    if(!isNaN(val) && val != -1) {
      PVPower[id] += val;
    } else {
      val = findVal(obj, 'PV1Power');
      PVPower[id] += isNaN(val)?0:val;
    }
    val = findVal(obj, 'MPPT2Power');
    if(!isNaN(val) && val != -1) {
      PVPower[id] += val;
    } else {
      val = findVal(obj, 'PV2Power');
      PVPower[id] += isNaN(val)?0:val;
    }
    val = findVal(obj, 'MPPT3Power');
    if(!isNaN(val) && val != -1) {
      PVPower[id] += val;
    } else {
      val = findVal(obj, 'PV3Power');
      PVPower[id] += isNaN(val)?0:val;
      val = findVal(obj, 'PV4Power');
      PVPower[id] += isNaN(val)?0:val;
      val = findVal(obj, 'PV5Power');
      PVPower[id] += isNaN(val)?0:val;
      val = findVal(obj, 'PV6Power');
      PVPower[id] += isNaN(val)?0:val;;
    }
    val = findVal(obj, 'PowerDC1');
    PVPower[id] += isNaN(val)?0:val;
    val = findVal(obj, 'PowerDC2');
    PVPower[id] += isNaN(val)?0:val;

    var val = findVal(obj, "TotalPVGeneration");
    if (val === undefined) {
      val = findVal(obj, 'ETotal');
    }
    if (val === undefined) {
      val = findVal(obj, 'AccumulatedEnergyYield');
    }
    if (val === undefined) {
      val = findVal(obj, 'TotalYield');
    }
    if (val === undefined) {
      val = 0;
    }
    PVEnergy[id] = val;

    var val = findVal(obj, "TodayPVGeneration");
    if (val === undefined) {
      val = findVal(obj, 'EDay');
    }
    if (val === undefined) {
      val = findVal(obj, 'DailyEnergyYield');
    }
    if (val === undefined) {
      val = 0;
    }
    todayPVEnergy[id] = val;
    
    val = findVal(obj, 'ActivePower');
    if (val === undefined) {
      val = findVal(obj, 'TotalInverterPower');
    }
    if (val === undefined) {
      val = findVal(obj, 'GridFeedingPowerL');
    }
    activePower[id] = val;

    if(!options.gridmeter) {
      val = findVal(obj, 'MTTotalActivePower');
      if(isFinite(val)) {
        gridBalance = -val;
        gridBalanceAge = Date.now();
        if(options.debug) {
          console.log(id, "gridBalance: ", gridBalance);
        }
      }
    }
    val = findVal(obj, "BatteryPower");
    if (val === undefined) {
      val = 0;
    }
    batteryPower[id] = val;
    if (options.debug) {
      console.log("PV-Inverter: GoodWe/Kostal/SMA/Huawei", id, " PVEnergy: ", PVEnergy[id], " TodayPVEnergy: ", todayPVEnergy[id], " PVPower:", PVPower[id], " ActivePower:", activePower[id], " Battery Power: ", batteryPower[id]);
    }
    sendAggregates();
  } else if (topic.includes("Hoymiles/")){
    let id = "Hoymiles";
    let found = false;

    if(topic.includes("ac/yieldtotal")) {
      var val = parseFloat(message);
      PVEnergy[id] = parseFloat(val.toFixed(3));
      found = true;
    }
    if(topic.includes("ac/yieldday")) {
      var val = parseFloat(message)/1000;
      todayPVEnergy[id] = parseFloat(val.toFixed(3));
      found = true;
    }
    if(topic.includes("ac/power")) {
      var val = parseFloat(message);
      PVPower[id] = parseFloat(val.toFixed(3));
      found = true;
    }
    if(found) {
      if(options.debug) {
        console.log("PV-Inverter Hoymiles: ", id, " yieldtotal: ", PVEnergy[id], " powerdc: ", PVPower[id]);
      }
      sendAggregates();
    }
  } else if (options.inverter.some(r => topic.includes(r))) {
    let id = topic.split('/')[1];
    let obj = JSON.parse(message);
    if(options.debug) {
      console.log(id + util.inspect(obj));
    }
    var val = findVal(obj, 'Power');
    PVPower[id] = isNaN(val)?0:val;
    val = findVal(obj, 'Total');
    PVEnergy[id] = isNaN(val)?0:val;
    val = findVal(obj, 'Today');
    todayPVEnergy[id] = isNaN(val)?0:val;
    if(options.debug) {
      console.log("PV-Inverter Solax: ", id, " yieldtotal: ", PVEnergy[id], " Today: ", todayPVEnergy[id],  " powerdc: ", PVPower[id]);
    }
  } else if ((index = options.evse.indexOf(topic)) >= 0) {
    let id = topic.split('/')[1];
    let obj = JSON.parse(message);
    let val = findVal(obj, options.evsefield[index]);
    if(val != undefined) {
      EVSEPower[id] = val*1000;
      if(options.debug) {
        console.log("EVSE: ",id, " TotalActivePower: ", EVSEPower[id]);
      }
    }
    sendAggregates();
  } else if ((index = options.dimmable.indexOf(topic)) >= 0) {
    let id = topic.split('/')[1];
    let obj = JSON.parse(message);
    let val = findVal(obj, options.dimmablefield[index]);
    if(val != undefined) {
      dimmablePower[id] = val*1000;
      if(options.debug) {
        console.log("Dimmable: ",id, " TotalActivePower: ", dimmablePower[id]);
      }
    }
    sendAggregates();
  } else if (topic.includes("SMAEM/") || topic.includes("tele/tasmota")) {
    let id = topic.split('/')[1];
    let obj = JSON.parse(message);
    val = findVal(obj, options.gridmeterfield);
    if (val === undefined && findVal(obj,'0:1.4.0') != undefined) {
      val = obj['0:1.4.0'] - obj['0:2.4.0'];
    }
    if(val != undefined) {
      gridBalance = val;
      gridBalanceAge = Date.now();
      if(options.debug) {
        console.log("gridBalance: ", id, "val: ", gridBalance);
      }
      sendAggregates();
    }
  }
});
