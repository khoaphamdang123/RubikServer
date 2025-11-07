import 'reflect-metadata'
import app from './app'
import * as http from 'http'
const normalizePort=require('normalize-port');
const bcrypt=require('bcrypt');
var port=normalizePort(process.env.Port||"9000");
app.set('port',port);
var server=http.createServer(app);
server.listen(port,()=>{console.log("server is listening really")});
server.on('error',onError);
server.on('listening',onListening);
function onError(error) {
    if (error.syscall !== "listen") {
      throw error;
    }
  
    var bind = typeof port === "string" ? "Pipe " + port : "Port " + port;
    switch (error.code) {
      case "EACCES":
        console.error(bind + " requires elevated privileges");
        process.exit(1);
        
      case "EADDRINUSE":
        console.error(bind + " is already in use");
        process.exit(1);
      default:
        throw error;
    }
  }
  const hashBcrypt=(password:string)=>
  { var password_hashed='';
    try
    {
    password_hashed=bcrypt.hashSync(password,8);
    }
    catch(error)
    {
      console.log(error.message);
    }
    return password_hashed;
  }
  function onListening() {
    var addr = server.address();    
    var bind = typeof addr === "string" ? "pipe " + addr : "port " + addr.port;
    var password_hashed=hashBcrypt("Helloman123456!@#");
    console.log(password_hashed);
    console.log("Server Running on Port:", port);
  }
