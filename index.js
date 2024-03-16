var util = require('util');
var mqtt = require('mqtt');
const commandLineArgs = require('command-line-args')
var errorCounter = 0;
var gridBalance = 0;
var PVPower = {};
var batteryPower = {};
var EVSEPower = {};
var activePower = {};
var PVEnergy = {};
var todayPVEnergy = {};
var startup = Date.now();
var nrg = {};
var totalLoadPower = 0;

const optionDefinitions = [
  { name: 'mqtthost', alias: 'm', type: String, defaultValue: "localhost" },
  { name: 'mqttclientid', alias: 'M', type: String, defaultValue: "mqtt2agg" },
  { name: 'inverter', alias: 'i', type: String, multiple: true, defaultValue: ['Huawei/#', 'GoodWe/#', 'Hoymiles/#'] },
  { name: 'gridmeter', alias: 'g', type: String },
  { name: 'evse', alias: 'e', type: String, multiple: true, defaultValue: ['tele/tasmota_9E1484/SENSOR', 'SM-DRT/EVSE2'] },
  { name: 'wait', alias: 'w', type: Number, defaultValue: 15000 },
  { name: 'debug', alias: 'd', type: Boolean, defaultValue: false },
  { name: 'goecharger', alias: 'E', type: String, multiple: true, defaultValue: ['+'] }
];

const options = commandLineArgs(optionDefinitions)

console.log("MQTT Host         : " + options.mqtthost);
console.log("MQTT Client ID    : " + options.mqttclientid);
console.log("Inverters         : " + options.inverter);
console.log("Grid meter        : " + options.gridmeter);
console.log("Go-eChargers      : go-eCharger/" + options.goecharger);

var MQTTclient = mqtt.connect("mqtt://" + options.mqtthost, { clientId: options.mqttclientid });
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
    console.log("subsribe: go-eCharger/" + address + "/...");
  }
  MQTTclient.subscribe("go-eCharger/" + address + "/nrg");
  MQTTclient.subscribe("go-eCharger/" + address + "/eto");
}

for (let address of options.inverter) {
  if (options.debug) {
    console.log("subsribe: " + address);
  }
  MQTTclient.subscribe(address);
}

for (let address of options.evse) {
  if (options.debug) {
    console.log("subsribe: " + address);
  }
  MQTTclient.subscribe(address);
}

MQTTclient.subscribe(options.gridmeter);

function sendAggregates() {
  if ((Date.now() - startup) > options.wait) {

    let totalPVPower = 0;
    let totalEVSEPower = 0;
    let totalActivePower = 0;
    let totalBatteryPower = 0;
    let load = 0;
    let totalPVEnergy = 0;
    let dayPVEnergy = 0;

    for (const [key, value] of Object.entries(PVPower)) {
      totalPVPower += value;
    }
    if (isNaN(totalPVPower)) {
      totalPVPower = 0;
    }
    for (const [key, value] of Object.entries(EVSEPower)) {
      totalEVSEPower += value;
    }
    if (isNaN(totalEVSEPower)) {
      totalEVSEPower = 0;
    }
    for (const [key, value] of Object.entries(activePower)) {
      totalActivePower += value;
    }
    if (isNaN(totalActivePower)) {
      totalActivePower = 0;
    }
    for (const [key, value] of Object.entries(PVEnergy)) {
      totalPVEnergy += value;
    }
    if (isNaN(totalPVEnergy)) {
      totalPVEnergy = 0;
    }
    for (const [key, value] of Object.entries(todayPVEnergy)) {
      dayPVEnergy += value;
    }
    if (isNaN(dayPVEnergy)) {
      dayPVEnergy = 0;
    }
    for (const [key, value] of Object.entries(batteryPower)) {
      totalBatteryPower += value;
    }
    if (isNaN(totalBatteryPower)) {
      totalBatteryPower = 0;
    }
    load = totalPVPower + gridBalance + totalBatteryPower;
    if (options.debug) {
      console.log("totalPVEnergy:", totalPVEnergy, "dayPVEnergy:", dayPVEnergy, " gridBalance: ", gridBalance, " BatteryPower: ", totalBatteryPower, " Load: ", load, " totalActivePower:", totalActivePower, " totalPVPower:", totalPVPower, " totalEVSEPower:", totalEVSEPower);
    }
    var state = {};
    state.totalPVPower = parseFloat(totalPVPower.toFixed(3));
    state.totalEVSEPower = parseFloat(totalEVSEPower.toFixed(3));
    state.totalActivePower = parseFloat(totalActivePower.toFixed(3));
    state.totalPVEnergy = parseFloat(totalPVEnergy.toFixed(3));
    state.dayPVEnergy = parseFloat(dayPVEnergy.toFixed(3));
    state.totalBatteryPower = parseFloat(totalBatteryPower.toFixed(3));
    state.load = parseFloat(load.toFixed(3));
    state.gridBalance = parseFloat(gridBalance.toFixed(3));
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
        sendMqtt("go-eCharger/" + id + "/agg", nrg);
      }
    } else if (func == 'eto') {
      nrg.eto = obj;
      if (options.debug) {
        console.log(util.inspect(nrg));
      }
      sendMqtt("go-eCharger/" + id + "/agg", nrg);
    }
  } else if (topic.includes("Huawei/") || topic.includes("GoodWe/") || topic.includes("Kostal/")) {
    let id = topic.split('/')[1];
    let obj = JSON.parse(message);
    if(options.debug) {
      console.log(id + util.inspect(obj));
    }
    var val = findVal(obj, 'PV1Power');
    PVPower[id] = isNaN(val)?0:val;
    val = findVal(obj, 'PV2Power');
    PVPower[id] += isNaN(val)?0:val;
    val = findVal(obj, 'PV3Power');
    PVPower[id] += isNaN(val)?0:val;
    val = findVal(obj, 'PV4Power');
    PVPower[id] += isNaN(val)?0:val;
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
        if(options.debug) {
          console.log("gridBalance: ", gridBalance);
        }
      }
    }
    val = findVal(obj, "BatteryPower");
    if (val === undefined) {
      val = 0;
    }
    batteryPower[id] = val;
    if (options.debug) {
      console.log("PV-Inverter: ", id, " PVEnergy: ", PVEnergy[id], " TodayPVEnergy: ", todayPVEnergy[id], " PVPower:", PVPower[id], " ActivePower:", activePower[id], " Battery Power: ", batteryPower[id]);
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
      console.log("Hoymiles: ", id, " yieldtotal: ", PVEnergy[id], " powerdc: ", PVPower[id]);
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
    console.log("Solax: ", id, " yieldtotal: ", PVEnergy[id], " Today: ", todayPVEnergy[id],  " powerdc: ", PVPower[id]);
  } else if (options.evse.indexOf(topic) >= 0) {
    let id = topic.split('/')[1];
    let obj = JSON.parse(message);
    EVSEPower[id] = findVal(obj, 'TotalActivePower');
    //    console.log("EVSE: ",id, " TotalActivePower: ", EVSEPower[id]);
    sendAggregates();
  } else if (topic.includes("SMAEM/") || topic.includes("tele/tasmota")) {
    let id = topic.split('/')[1];
    let obj = JSON.parse(message);
    val = findVal(obj, 'Power');
    if (val === undefined && findVal(obj,'0:1.4.0') != undefined) {
      gridBalance = obj['0:1.4.0'] - obj['0:2.4.0'];
    } else {
      gridBalance = val;
    }
    if(options.debug) {
      console.log("gridBalance: ", gridBalance);
    }
    sendAggregates();
  }
});
