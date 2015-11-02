//###############################################################################
//##
//# Copyright (C) 2014-2015 Andrea Rocco Lotronto
//##
//# Licensed under the Apache License, Version 2.0 (the "License");
//# you may not use this file except in compliance with the License.
//# You may obtain a copy of the License at
//##
//# http://www.apache.org/licenses/LICENSE-2.0
//##
//# Unless required by applicable law or agreed to in writing, software
//# distributed under the License is distributed on an "AS IS" BASIS,
//# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//# See the License for the specific language governing permissions and
//# limitations under the License.
//##
//###############################################################################
  
var WebSocketServer, bindSockets, http, net, url, wst_server_reverse;

WebSocketServer = require('websocket').server;
http = require('http');
url = require("url");
net = require("net");
bindSockets = require("./bindSockets_reverse");

var eventEmitter = require('events').EventEmitter;

var newWSTCP_DATA = new eventEmitter();

var handler = {};

wst_server_reverse = function(dstHost, dstPort) {

  this.dstHost = dstHost;
  this.dstPort = dstPort;
  
  this.httpServer = http.createServer(function(request, response) {
    console.log((new Date()) + ' Received unhandled request for ' + request.url);
    response.writeHead(404);
    return response.end();
  });
      
  this.wsServerForControll = new WebSocketServer({
    httpServer: this.httpServer,
    autoAcceptConnections: false
    });
}

wst_server_reverse.prototype.on = function(message, callback) {
  handler[message] = callback;
}

wst_server_reverse.prototype.start = function(port) {

  this.httpServer.listen(port, function() {
    return console.log((new Date()) + " Server is listening on port " + port);
  });
  
  this.wsServerForControll.on('request', (function(_this){
    return function(request){
      //One TCP Server for each client WS Request
      request.tcpServer = new net.createServer();

      var uri = url.parse(request.httpRequest.url, true);
      
      if (uri.query.dst != undefined){
        
        var remoteAddr = uri.query.dst;
        var hostComodo, portTcp;
        ref1 = remoteAddr.split(":"), hostComodo = ref1[0], portTcp = ref1[1];

        if('ws_open' in handler && !handler['ws_open'](hostComodo, portTcp)) return false;
     
        request.tcpServer.listen(portTcp);
        
        console.log("Created TCP server on port "+portTcp);
        
        request.wsConnectionForControll = request.accept('tunnel-protocol', request.origin);
        //DEBUG MESSAGE FOR TESTING
        console.log("WS Connectio for Control Created");

        request.wsConnectionForControll.on('close', function(reasonCode, description) {
          if('ws_close' in handler) handler['ws_close'](hostComodo, portTcp);
          console.log((new Date()) + 'WebSocket Controll Peer ' + request.wsConnectionForControll.remoteAddress + ' disconnected for:\"'+description+'\"');
          console.log((new Date()) + "Close TCP server on port "+portTcp);
          request.tcpServer.close();
        });
      }
      //REQUEST FOR WS SOCKET USED FOR DATA
      else{ 
        console.log("Request for Data WS Socket");
        newWSTCP_DATA.emit('created', request);
      }

      //Manage TCP Connection
      request.tcpServer.on('connection', (function(_this){
        
        return function(tcpConn){
          tcpConn.wsConnection;
          //Putting in pause the tcp connection waiting the new socket WS Socket for data
          tcpConn.pause();
        
          var idConnection = randomIntInc(1,1000);
          var msgForNewConnection = "NC:"+idConnection;
          
          request.wsConnectionForControll.sendUTF(msgForNewConnection);
  
          newWSTCP_DATA.on('created',(function(_this){
          return function(request){
            //DEBUG MESSAGE FOR TESTING
            //console.log(typeof(_this.wsConnection));            
            var uri = url.parse(request.httpRequest.url, true);
            //DEBUG MESSAGE FOR TESTING
            //console.log("TEEEEEEEEESTTTT::"+uri.query.id);
            if(idConnection == uri.query.id){
              //DEBUG MESSAGE FOR TESTING
              //console.log("TRUE")   
              //tcpConn.wsConnection = wsTCP;
              tcpConn.wsConnection = request.accept('tunnel-protocol', request.origin);
              bindSockets(tcpConn.wsConnection,tcpConn);
              //DEBUG MESSAGE FOR TESTING
              //console.log("Bind ws tcp");
              //Resuming of the tcp connection after WS Socket is just created
              tcpConn.resume();
              //DEBUG MESSAGE FOR TESTING
              //console.log("TCP RESUME");
              }
            }
          })(this));  
        }
      })(_this));

    }
  })(this));
};

function randomIntInc (low, high) {
    return Math.floor(Math.random() * (high - low + 1) + low);
}

module.exports = wst_server_reverse;