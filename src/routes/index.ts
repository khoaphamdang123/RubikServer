import * as express from 'express'
import {user,room_user,user_room_detail,rubik_info,image_detail,social_account,session,role,rubikProblem,rubikProblemDetail,temp_device,device,category,rubikType,feedback} from '../models/user_model';
import checkingDuplicateUserNameOrEmail from '../config/checking';
import {token_checking,email_token_checking,admin_checking,user_only_checking} from '../config/checkingToken';
import {username,password,loginUrl,registerServerUrl} from './gmail_account';
import { DateTime, Interval } from 'luxon';
import { Kafka } from 'kafkajs';
import { Body } from 'twilio/lib/twiml/MessagingResponse';
import { resolveSoa } from 'dns';
import { text } from 'stream/consumers';
import { Console } from 'console';
const logger=require('../logger/index');
var router = express.Router();
var config=require('../config/auth');
var jwt=require('jsonwebtoken');
var nodemailer=require('nodemailer');
/* GET home page. */
require('dotenv').config();
const bcrypt=require('bcrypt');
const crypto=require('crypto');
const uuid=require('uuid');
const cheerio=require('cheerio');
var axios= require('axios');
const Cube= require('cubejs');
const fs=require('fs');
const twilio = require('twilio');
const multer = require('multer');
const account_sid=process.env.ACCOUNT_SID;
const authenticate_token=process.env.AUTHENTICATION_TOKEN;
const twilio_phone=process.env.TWILIO_PHONE;
const twilio_client = new twilio(account_sid,authenticate_token);
const INFOBIP_API_BASE_URL=process.env.INFOBIP_API_BASE_URL;
const API_KEY=process.env.API_KEY;
const path=require('path');
const fs_promise = require('fs').promises;

const kafka=new Kafka({
  clientId:"Rubik-BE",
  brokers:[process.env.KAFKA_BROKER,process.env.KAFKA_BROKER]
});
// const storage = multer.diskStorage(
//   {
//   destination:(req,file,cb)=>{
//     cb(null,'uploads/');
//   },
//   filename:(req,file,cb)=>{
//      cb(null,Date.now()+path.extname(file.originalname));
//   }
// })
//const upload=multer({storage:storage});
const upload = multer({ dest: 'upload/' });

const producer=kafka.producer();
const consumer = kafka.consumer({groupId:'Rubik-BE'});
const admin= kafka.admin();
var list_topic=[];
var subscribe_list=[];
var received_message='';

const mqttInit=async()=>{
  try
  {    
   await producer.connect();
   await consumer.connect();
   await admin.connect();
   var list_device=await device.find();
   for(const device of list_device)
    {
      var topic=device.device_name;
      var username=device.username;
      var topic_name=`${username}_${topic}`;
      const ob_topic=
      {topic:topic_name,
      numPartitions: 1,
      replicationFactor: 1
      };
      console.log(JSON.stringify(ob_topic)+"\n");
      list_topic.push(ob_topic);
      subscribe_list.push(topic_name);
    }
    await admin.createTopics({
      waitForLeaders:true,
      topics:list_topic
    });
   console.log('Topic created successfully');

   await consumer.subscribe({topics:subscribe_list,fromBeginning:true});
   await consumer_run();
  }
  catch(err)
  { console.log("Broker Server is:"+process.env.KAFKA_BROKER);
    console.log("MQTT INIT FAILED:"+err.message);
    logger.error("MQTT INIT FAILED:"+err.message);
  }
}

const autoUpdateTopicList=()=>
{
   try
   {
       setInterval(async()=>{
       var new_device_list=await temp_device.find({});
       var list_new_topic=[];
       var subscribe_new_topic=[];
       await admin.disconnect();
       await consumer.disconnect();

      for(let device of new_device_list)
        { 
          var username = device.username;
          var device_name=device.device_name;
          var topic_name=`${username}_${device_name}`;
          // var created_date= device.created_date;
          // var status=false;
          // var online_time=created_date;
          const ob_topic =
          {
           topic:topic_name,
           numPartitions:1,
           replicationFactor:1
          };
          list_new_topic.push(ob_topic);
          subscribe_new_topic.push(topic_name);
        }
        await admin.connect();
        await consumer.connect();
        await admin.createTopics({
          waitForLeaders:true,
          topics:list_topic
        });
        await consumer.subscribe({topics:subscribe_list,fromBeginning:true});
        await consumer_run();
        await temp_device.deleteMany({});
        console.log("Update Topic List Successful");
        logger.info("Update Topic List Successfully.");
    },720000);
   }
   catch(ex)
   {
    console.log("Auto Update Exception:"+ex.message);
    logger.error("Auto Update Exception:"+ex.message);
   }
}


const deleteAllTopics=async()=>
{
  try
  { await admin.connect();
    var topics = await admin.listTopics();
    
    for(const topic of topics)
      {
     await admin.deleteTopics({
      topics:[topic],
      timeout:5000
     });
      }
    await admin.disconnect();
  
  console.log("delete topics successfully");
  logger.info("Delete all topics successfully");
    }
  catch(err)
  {
    console.log("Delete All Topics Exception:"+err.message);
    logger.error("Delete All Topics Exception:"+err.message);
   }
  }


  const deleteAllLogsFiles=async(directory:string)=>
  {
  try
  {
  fs.readdir(directory,(err,files)=>
  {
    if(err)
      {
        throw err;
      }
    files.forEach((file)=>{
      const file_path=path.join(directory,file);
      fs.stat(file_path,(err,file_stats)=>{
         if(err)
          {
            throw err;
          }
         if(file_stats.isFile())
          {
            fs.unlink(file_path,(err)=>{
              if(err)
                {
                  throw err;
                }
            });
          }
        else if(file_stats.isDirectory())
        {
          fs.rmSync(file_path,{recursive:true,force:true});
        } 
      })
    })
  })
  }
  catch(ex)
  {
    console.log('Delete All Logs File Exception:'+ex.message);
    logger.error('Delete All Logs File Exception:'+ex.message);
  }
  }


const rotateSerectKey=()=>
{
  try
  {
   config.secret=Math.random().toString(36).slice(2);
   logger.info("Rotate Secret Key Successfully.");
  }
  catch(ex)
  {
    console.log("Rotate Secret Key Exception:"+ex.message);
    logger.error("Rotate Secret Key Exception:"+ex.message);
  }
}

const checkValidPhone=(phone:string):boolean=>
{
  const pattern = /^[+]{1}(?:[0-9\-\\(\\)\\/.]\s?){6,15}[0-9]{1}$/;
  var reg=new RegExp(pattern);
  return reg.test(phone);
}
const deleteHandledImage=async(images:string[])=>
{
  try
  {
   var cwd=path.join(process.cwd(),'upload/');
   for(let img of images)
    { console.log("image here is:"+img);
      var img_path=path.join(cwd,img);
      await fs_promise.unlink(img_path);
      logger.info("DELETE IMAGE "+img+" SUCCESSFULLY");
    }
   console.log("current working path is:"+cwd);
  }
  catch(err)
  {
    console.log("DELETE IMAGE EXCEPTION:"+err.message);
    logger.error("DELETE IMAGE EXCEPTION:"+err.message);
  }
};


const convertQrToOtp=(qr:string)=>{
  var otp='';
  for(let i =0;i<qr.length;i++)
    {
      var qr_value= qr[i].charCodeAt(0)-65;
      var otp_val=qr_value%10;
      otp+=otp_val;
    }
    return otp;
};

const checkPassword=(password:string):boolean=>{
  const pattern= /^(?=.*\d)(?=.*[!@#$%^&*])(?=.*[a-z])(?=.*[A-Z]).{10,}$/;
  var reg=new RegExp(pattern);
  return reg.test(password);
}



//rotateSerectKey();
//deleteAllLogsFiles('C:/kafka/config/kafka-logs');
//deleteAllTopics();
mqttInit();
autoUpdateTopicList();

const transportEmail=nodemailer.createTransport({
    service:'gmail',
    auth:{
      user:username,
      pass:password
    }
  });

const hbs=require('nodemailer-express-handlebars');
const handlebarsOption={
    viewEngine :
    {
        partialsDir: path.resolve('../sudokusv/src/views/'),
        defaultLayout: false,
    },
    viewPath:path.resolve('../sudokusv/src/views/') 
};

transportEmail.use('compile',hbs(handlebarsOption));


 router.post('/verify',checkingDuplicateUserNameOrEmail,function(req,res,next){
    var new_user=req.body;
    let email_payload=
    {
      username:new_user.username,
      email:new_user.email
    };

    let email_token=jwt.sign(email_payload,config.secret,{expiresIn:300});
    console.log(email_token);
    console.log('verify:'+new_user.username+' '+new_user.password+' '+new_user.gender+' '+new_user.email);    
    const emailContentConfig={
        from:'huynhkiengquan@gmail.com',
        template:'email_template',
        to:new_user.email,
        subject:'Email verification',
        context:{
            username:new_user.username,
            link:`${registerServerUrl}?username=${new_user.username}&password=${encodeURIComponent(new_user.password)}&gender=${new_user.gender}&email=${new_user.email}&token=${encodeURIComponent(email_token)}`
        }
       };
       transportEmail.sendMail(emailContentConfig,(error,info)=>{
        if(error)
        {  
           throw Error(error);
        }
        console.log("Send mail successfully:"+info);
       });
       res.status(200).send({'message':'Vui lòng vào email của bạn để xác thực tài khoản.'});
 });



const colorToFace=(color:string)=>
{
 try
 { 
  var res='';
  switch(color)
  {
    case 'whitesmoke':res='U';break;
    case 'orange':res='L';break;
    case 'green': res='F';break;
    case 'red':res='R';break;
    case 'blue':res='B';break;
    case 'yellow':res='D';break;
    default:res='';break;
  }
  return res;
 }
 catch(err)
 { logger.error('Color to Face error:'+err.message);
  return err;
 }
}

const convertRubikAnno=(colors:string[])=>
{
  try
  { 
    var res='';

     for(let color of colors)
     { 
       var convert_color=colorToFace(color);

       if(convert_color!='')
       {
        res+=convert_color;
       }
     }
     return res;          
  }
  catch(error)
  { 
    logger.error('Convert Rubik Anno error:'+error.message);
    return error.message;
  }
};

router.post('/register',email_token_checking,function(req, res, next) {
   const new_user=
   {
    username:'',
    password:'',
    gender:'',
    email:'',
    avatar:'',
    created_date:'',
    display_name:'',
    motto:''
   };
   new_user.username=req.query.username;
   new_user.password=bcrypt.hashSync(req.query.password,8);
   new_user.gender=req.query.gender;
   new_user.email=req.query.email;
   new_user.avatar='https://gw.alipayobjects.com/zos/rmsportal/BiazfanxmamNRoxxVxka.png';
   new_user.created_date=DateTime.now().toLocaleString(DateTime.DATE_FULL);
   new_user.display_name=req.query.username;
   console.log('register:'+new_user.username+' '+new_user.password+' '+new_user.gender+' '+new_user.email);
   if(new_user.username==='' || new_user.password===''||new_user.gender===''||new_user.email==='')
   {
    res.redirect(301,`${loginUrl}?email=${new_user.email}`);
   }
   else
   {
   const register_user=new user(new_user);
   try{
    register_user.save((err,doc)=>{
        if(err)
        {
            return console.error(err);
        }
        res.redirect(301,`${loginUrl}?email=${new_user.email}`);
    });
    console.log(new_user);
   }
   catch(error)
   {
    res.status(404).json({message:error});
   }
}
}
);

router.get('/login',function(req,res,next){
    try{
        var email=req.query.email;
        user.findOne({email:email}).exec((err,user_valid)=>{
         if(err)
         {
            throw err
         }
         if(user_valid)
         {
            res.status(201).send({message:"Đã đăng ký thành công"});
         }
         else
         {
            res.status(400).send({message:"Đăng ký thất bại."});
         }
        });
    }
    catch(error)
    {
    logger.error('Get Login error:'+error.message);
    res.status(404).json({message:error});
    }
});

router.get('/add-account',token_checking,function(req,res,next){
  try
  {
    res.status(200).send({status:true,message:'Get page successfully.'});
  }
  catch(error)
  {
  res.status(401).send({status:false,message:error.message});
  }
});


router.post('/add-account',async function(req,res,next)
{
try
{  
  var username=req.body.username;
  var password=bcrypt.hashSync(req.body.password,8);
  var gender=req.body.gender;
  var email=req.body.email;
  var avatar_url=req.body.avatar;
  var role_id=req.body.role_id;

  var check_exist=await user.find({$or:[{username:username},{email:email}]}).exec((err,data)=>{
   if(err)
   {
    throw err;
   }

   if(data[0].username==username)
   {
     return res.status(401).send({status:false,message:'This username existed in the system.'});
   }
   else
   {
   return res.status(401).send({status:false,message:'This email existed in the system.'});
   }
  });

  var account_obj=
  {
  username:username,
  password:password,
  gender:gender,
  email:email,
  avatar:avatar_url,
  created_date:new Date().toLocaleString(),
  role_id:role_id,
  }
  var account=new user(account_obj);
  account.save((err,data)=>{
   if(err)
   {
    throw(err);
   } 
  return res.status(200).send({status:true,message:'Add account successfully',data:data});
  });
}
catch(err)
{
  console.log('There is error while adding new account');
  return res.status(401).send({status:false,message:err.message});
}
});

router.get('/device/:username',user_only_checking,async function(req,res,next)
{
  try
  {
   var username =req.params.username;
   await device.find({username:username}).exec((err,devicee)=>{
     if(err)
      { 
        logger.success(`GET DEVICE LIST FAILED FOR USER ${username}:${err}`);
        res.status(400).send({status:false,message:err.message});
      }
      logger.info(`GET DEVICE LIST SUCCESSFULLY FOR USER ${username}`);
      res.status(200).send({status:true,message:devicee});
   });
  }
  catch(err)
  {
    console.log("Exception occured when getting device list:"+err.message);
    logger.error("EXCEPTION GETTING DEVICE LIST:"+err.message);
    
    res.status(400).send({status:false,message:err.message});
  }
});




router.post('/add_images',user_only_checking,upload.array('images',10),async function(req,res,next)
{
try
{ 
  var img_files=req.files;
  var img_files_name=[];
  var color_images=[];
  var index=-1;
  var original_cube=req.body.original_cube;
  console.log('original_cube here is:'+original_cube);
  var formData=new FormData();
  for(let img of img_files)
    { logger.info("Image original name:"+img.originalname);
      logger.info("Image file name:"+img.filename);
      console.log("image info:"+JSON.stringify(img));
      index+=1;
      img_files_name.push(img.filename);
      console.log('image file path:'+img.path);
      const img_path=path.join(process.cwd(),img.path);
      const file_data=await fs_promise.readFile(img_path);
      const blob = new Blob([file_data],{type:img.mimetype});
      console.log("blob file obj:"+JSON.stringify(blob));
      formData.append('img',blob,img.originalname);
      console.log(img.filename);
    }
    formData.append("original_cube",original_cube);
    var response = await axios.post(`${process.env.THIRD_PARTY_IP}/color_detection_image/`,formData,{headers:{'Content-Type': 'multipart/form-data'}}).then((res)=>{
      console.log("Response from third party;"+res.data.message);
      logger.info("Third party handle image:"+res.data.message);
      color_images.push(res.data.data);
    }).catch(err=>{console.log("There is error while sending image to third-party"+err.message);
     throw err;
    });
  
    res.status(200).send({'status':true,'data':color_images});
      

  // console.log('file here is:'+img_files);
  
  // var img_face_name=req.body.arr;

  logger.info('ADD IMAGES SUCCESSFULLY.'); 
  await delay(5000); 
  deleteHandledImage(img_files_name);
 //res.status(200).send({status:true,message:'Add images successfully.'});
  
}
catch(ex)
{
  console.log("EXCEPTION ADDING IMAGES:"+ex.message);
  logger.error("EXCEPTION ADDING IMAGES:"+ex.message);
  res.status(400).send({status:false,message:'Error receiving images:'+ex.message});
}
});

router.post('/add_device',user_only_checking,async function(req,res,next)
{
try
{
  var device_name=req.body.device_name;
  var user_name=req.body.username;
  var checkExistingDevice = await device.findOne({$and:[{device_name:device_name},{username:user_name}]});
  if(checkExistingDevice)
    {  logger.error(`DEVICE ${device_name} HAS EXISTED`);
      res.status(400).send({status:false,message:'This device name has existed in this user device list'});
    } 
  else
  {
   var created_date=DateTime.now().toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
   await device.create({username:user_name,device_name:device_name,created_date:created_date,status:false,online_time:created_date});
   await temp_device.create({username:user_name,device_name:device_name,created_date:created_date});
   logger.info(`ADD DEVICE ${device_name} SUCCESSFULLY FOR USER ${user_name}.Your device will be available to use in the next day.`);
   res.status(200).send({status:true,message:'Add new device successfully'});
  }
}
catch(err)
{
  console.log("Exception occured when adding device:"+err.message);
  logger.error("EXCEPTION ADDING DEVICE:"+err.message);
  res.status(400).send({status:false,message:err.message});
  
}
});


router.post('/delete_device',user_only_checking,async function(req,res,next){
try
{
  var device_name=req.body.device_name;
  var username=req.body.username;
  
  await device.deleteOne({$and:[{username:username},{device_name:device_name}]}).exec((err,deleted_device)=>{
    if(err)
      { 
        logger.error(`DELETE DEVICE ${deleted_device} FAILED:${err}`);
        res.status(400).send({status:false,message:err.message});
      }
  logger.info(`DELETE DEVICE ${device_name} SUCCESSFULLY FROM USER ${username}`);
  res.status(200).send({status:true,message:'Delete successfully'});
  });
}
catch(err)
{
  console.log("Exception occured when deleting device:"+err.message);
  logger.error("EXCEPTION DELETING DEVICE:"+err.message);
  res.status(400).send({status:false,message:err.message});
}
});

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

router.post('/login',function (req,res,next){
 try
 {
   console.log("Username here is:"+req.body.username);
   console.log("Password here is:"+req.body.password);
   var ip_addr=req.body.ip_addr;
   var city=req.body.city;
   var type=req.body.type;
  //   var user_object=
  //   {
  //    username:'helloman123',
  //    password:'$2b$08$w1sjTXM8kcjfDxaBPRJtP.8a.CMKZzqpGQE9LRhjPhV/L3BRIThC2',
  //    email:'helloman123@gmail.com',
  //    gender:'male',
  //    avatar:'https://gw.alipayobjects.com/zos/rmsportal/BiazfanxmamNRoxxVxka.png',
  //    created_date:new Date().toLocaleString(),
  //    role_id:0
  //   };
  //   var role_ob=
  //   {
  //    role_type:'Admin'
  //   };
  //   var role_user=
  //   {
  //     role_type:'User'
  //   };
  //   var create_role_admin=new role(role_ob);
  //   var create_role_user= new role(role_user);
  //   create_role_admin.save((err,data)=>{
  //     if(err)
  //     {
  //       throw err;
  //     }
  //   });
   
  //   create_role_user.save((err,data)=>{
  //  if(err)
  //  {
  //   throw err;
  //  }
  //   });
  //   var create_user=new user(user_object);
  //   create_user.save((err,data)=>{
  //     if(err)
  //     {
  //       throw err;
  //     }
  //   });
   if(type!=null)
    {
      console.log("Type value here is:"+type);
      var username=req.body.username;
      var password=req.body.password;
      if(type=='Google')
        {
      user.findOne({email:username}).exec(async(err,userr)=>{
        if(err)
          {    
              console.log("Error while fetching user");
              return;
          }
          if(!userr)
            {
           var created_date=DateTime.now().toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
            var hash_password=hashBcrypt(password);
            await social_account.create({username:username,display_name:username,social_type:type});
            await user.create({username:username,gender:'undefined',email:username,phone:'01217926739',avatar:'https://gw.alipayobjects.com/zos/rmsportal/BiazfanxmamNRoxxVxka.png',
            created_date:created_date,last_active:created_date,last_action:created_date,is_checking:false,role_id:0,password:hash_password});
            var user_ob=await user.findOne({username:username});
            const jwt_payload=
            {
                user_id:user_ob._id,
                username:user_ob.username
                
            }
      
            var token=jwt.sign(jwt_payload,config.secret,{expiresIn:'1h'});
            const existingToken=await session.findOne({user_name:username});
            var created_time=DateTime.now().toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
            if(existingToken)
              {
               await session.updateOne({user_name:username},{$set:{token:token,created_time:created_time,ip_address:ip_addr,city:city}});
              }
            else
            {
             await session.create({user_name:username,token:token,created_time:created_time,ip_address:ip_addr,city:city});
            }
            req.session.token=token;
            return res.status(200).send({message:"Đăng nhập thành công",token:req.session.token,data:user_ob});
            }
            else
            {
              var passwordIsValid=bcrypt.compareSync(req.body.password,userr.password);
              if(!passwordIsValid)
              {  
                  return res.status(401).send({message:"Password is invalid"});
              }
              
              const jwt_payload=
              {
                  user_id:userr.id,
                  username:userr.username
              }
          
              var token=jwt.sign(jwt_payload,config.secret,{expiresIn:'1h'});
              var created_time=DateTime.now().toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
              const existingToken=await session.findOne({user_name:userr.username});
              if(existingToken)
                {
                 await session.updateOne({user_name:userr.username},{$set:{token:token,created_time:created_time,ip_address:ip_addr,city:city}});
                }
              else
              {
               await session.create({user_name:userr.username,token:token,created_time:created_time,ip_address:ip_addr,city:city});
              }
              req.session.token=token;
              return res.status(200).send({message:"Đăng nhập thành công",token:req.session.token,data:userr});
            }
      });    
        }
       return;
    }
        
    user.findOne({username:req.body.username}).exec(async(err,userr)=>{
        if(err)
        {    
            console.log("Error while fetching user");
            return;
        }
        if(!userr)
        {  
          return res.status(401).send({message:"Username do not exist"});
        }
    var passwordIsValid=bcrypt.compareSync(req.body.password,userr.password);
    if(!passwordIsValid)
    {  
        return res.status(401).send({message:"Password is invalid"});
        
    }
    
    const jwt_payload=
    {
        user_id:userr.id,
        username:userr.username
    }

    var token=jwt.sign(jwt_payload,config.secret,{expiresIn:'1h'});
    var created_time=DateTime.now().toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
    const existingToken=await session.findOne({user_name:userr.username});
    if(existingToken)
      {
       await session.updateOne({user_name:userr.username},{$set:{token:token,created_time:created_time,ip_address:ip_addr,city:city}});
      }
    else
    {
     await session.create({user_name:userr.username,token:token,created_time:created_time,ip_address:ip_addr,city:city});
    }
    req.session.token=token;
    return res.status(200).send({message:"Đăng nhập thành công",token:req.session.token,data:userr});
    
    })
  }
  catch(err)
  {
    res.status(401).send({status:false,message:err.message});
  }
});

// 管理员登录API - 只允许管理员用户登录
router.post('/admin/login',async function(req,res,next){
  try
  {
    console.log("Admin login attempt - Username: "+req.body.username);
    var ip_addr=req.body.ip_addr;
    
    var city=req.body.city;
    
    if(!req.body.username || !req.body.password)
    {
      return res.status(400).send({status:false,message:"Username and password are required"});
    }
    
    // 查找用户
    var userr=await user.findOne({username:req.body.username});
    
    if(!userr)
    {
      logger.error(`Admin login failed: Username ${req.body.username} does not exist`);
      return res.status(401).send({status:false,message:"Invalid username or password"});
    }
    
    // 验证密码
    var passwordIsValid=bcrypt.compareSync(req.body.password,userr.password);
    if(!passwordIsValid)
    {
      logger.error(`Admin login failed: Invalid password for user ${req.body.username}`);
      return res.status(401).send({status:false,message:"Invalid username or password"});
    }
    
    // 检查用户角色 - 必须是管理员
    var user_role=await role.findOne({_id:userr.role_id});
    if(!user_role)
    {
      logger.error(`Admin login failed: Role not found for user ${req.body.username}`);
      return res.status(403).send({status:false,message:"User role not found"});
    }
    
    // 验证是否是管理员角色
    if(user_role.role_type!=='Admin')
    {
      logger.error(`Admin login failed: User ${req.body.username} is not an admin (role: ${user_role.role_type})`);
      return res.status(403).send({status:false,message:"Access denied. Admin privileges required."});
    }
    
    // 生成JWT token
    const jwt_payload=
    {
      user_id:userr._id,
      username:userr.username
    }
    
    var token=jwt.sign(jwt_payload,config.secret,{expiresIn:'1h'});
    var created_time=DateTime.now().toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
    
    // 更新或创建session
    const existingToken=await session.findOne({user_name:userr.username});
    if(existingToken)
    {
      await session.updateOne({user_name:userr.username},{$set:{token:token,created_time:created_time,ip_address:ip_addr,city:city}});
    }
    else
    {
      await session.create({user_name:userr.username,token:token,created_time:created_time,ip_address:ip_addr,city:city});
    }
    
    // 更新用户最后活动时间
    // var last_active_action=userr.last_active;
    // var now_str=DateTime.now().toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
    // await user.updateOne({username:userr.username},{$set:{last_active:now_str,last_action:last_active_action}});
    
    logger.info(`Admin login successful: ${userr.username}`);
    
    console.log(`Admin login successful: ${userr.username}`);

    return res.status(200).send({
      status:true,
      message:"Admin login successful",
      token:token,
      avatar:userr.avatar,
      data:{
        id:userr._id,
        username:userr.username,
        email:userr.email,
        role:user_role.role_type
      }
    });
  }
  catch(err)
  {
    logger.error("Admin login error: "+err.message);
    res.status(500).send({status:false,message:"Internal server error"});    
  }
});

// 管理员仪表板API - 获取管理员仪表板统计数据
router.get('/admin/dashboard',admin_checking,async function(req,res,next){
  try
  {
    // 获取用户统计
    const totalUsers = await user.countDocuments();
    
    // 获取所有管理员角色ID
    const adminRoles = await role.find({role_type: 'Admin'});
    
    const adminRoleIds = adminRoles.map(r => r._id);
    
    // 统计管理员用户数
    const adminCount = await user.countDocuments({role_id: {$in: adminRoleIds}});
    
    const regularUserCount = totalUsers - adminCount;

    // 获取活跃会话统计
    const activeSessions = await session.countDocuments();
    
    // 获取设备统计
    const totalDevices = await device.countDocuments();
    
    // 获取房间统计（如果存在）
    let totalRooms = 0;
    let activeRooms = 0;
    try {
      totalRooms = await room_user.countDocuments();
      // 可以根据需要定义活跃房间的逻辑
      activeRooms = await room_user.countDocuments();
    } catch(err) {
      // 如果room_user模型不存在或出错，忽略
      logger.warn("Room statistics not available: "+err.message);
    }

    // 获取Rubik问题统计
    let totalRubikProblems = 0;
    try {
      totalRubikProblems = await rubikProblem.countDocuments();
    } catch(err) {
      logger.warn("Rubik problem statistics not available: "+err.message);
    }

    // 获取最近24小时注册的用户数（created_date是字符串格式，使用简化处理）
    const yesterday = DateTime.now().minus({ hours: 24 });
    const yesterdayTimestamp = yesterday.toMillis();
    const allUsers = await user.find({}, {created_date: 1});
    const recentUsers = allUsers.filter(u => {
      if (!u.created_date) return false;
      try {
        // 尝试解析日期字符串
        let userDate = DateTime.fromISO(u.created_date);
        if (!userDate.isValid) {
          userDate = DateTime.fromSQL(u.created_date);
        }
        if (userDate.isValid) {
          return userDate.toMillis() >= yesterdayTimestamp;
        }
        return false;
      } catch {
        return false;
      }
    }).length;

    // 获取最近24小时活跃会话数（created_time是字符串格式，使用简化处理）
    const allSessions = await session.find({}, {created_time: 1});
    const recentSessions = allSessions.filter(s => {
      if (!s.created_time) return false;
      try {
        // 尝试解析日期字符串
        let sessionDate = DateTime.fromISO(s.created_time);
        if (!sessionDate.isValid) {
          sessionDate = DateTime.fromSQL(s.created_time);
        }
        if (sessionDate.isValid) {
          return sessionDate.toMillis() >= yesterdayTimestamp;
        }
        return false;
      } catch {
        return false;
      }
    }).length;

    // 获取当前登录的管理员信息
    const currentAdmin = await user.findOne({username: req.username});
    
    const adminRole = await role.findOne({_id: currentAdmin.role_id});

    logger.info(`Admin dashboard accessed by: ${req.username}`);

    return res.status(200).json({
      status: true,
      message: "Dashboard data retrieved successfully",
      data: {
        userStatistics: {
          totalUsers: totalUsers,
          adminUsers: adminCount,
          regularUsers: regularUserCount,
          recentRegistrations: recentUsers
        },
        sessionStatistics: {
          activeSessions: activeSessions,
          recentSessions: recentSessions
        },
        deviceStatistics: {
          totalDevices: totalDevices
        },
        roomStatistics: {
          totalRooms: totalRooms,
          activeRooms: activeRooms
        },
        rubikStatistics: {
          totalProblems: totalRubikProblems
        },
        currentAdmin: {
          id: currentAdmin._id,
          username: currentAdmin.username,
          email: currentAdmin.email,
          role: adminRole ? adminRole.role_type : 'Unknown',
          lastActive: currentAdmin.last_active
        },
        timestamp: DateTime.now().toISO()
      }
    });
  }
  catch(err)
  {
    logger.error("Admin dashboard error: "+err.message);
    res.status(500).json({
      status: false,
      message: "Internal server error",
      error: err.message
    });
  }
});

// 管理员用户列表API - 获取用户列表（所有用户类型）

router.get('/admin/users', admin_checking, async function(req, res, next) {
  try {
    // 获取查询参数
    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 10;
    const search = req.query.search as string || '';
    const roleFilter = req.query.role as string || '';    
    
    // 计算跳过的记录数
    const skip = (page - 1) * limit;
  
    // 构建查询条件数组
    const conditions: any[] = [];

    // 构建role_id条件
    let roleIdCondition: any = {};
    
    // 角色过滤（包括所有角色类型，包括Admin）
    if (roleFilter) {
      // 查找匹配的角色ID（包括Admin角色）
      const roles = await role.find({ 
        role_type: { $regex: roleFilter, $options: 'i' }
      });
      
      const roleIds = roles.map(r => r._id);
      
      if (roleIds.length > 0) {
        roleIdCondition.$in = roleIds;
      } else {
        // 如果没有匹配的角色，返回空结果
        roleIdCondition.$in = [];
      }
    }
    
    // 如果有role_id条件，添加到查询中
    if (Object.keys(roleIdCondition).length > 0) {
      conditions.push({ role_id: roleIdCondition });
    }
    
    // 搜索条件：按用户名或邮箱搜索
    if (search) {
      conditions.push({
        $or: [
          { username: { $regex: search, $options: 'i' } },
          { email: { $regex: search, $options: 'i' } }
        ]
      });
    }
    
    // 构建最终查询
    const query = conditions.length > 0 ? { $and: conditions } : {};
    
    // 获取总数
    const totalUsers = await user.countDocuments(query);
    
    // 获取用户列表（排除密码字段）
    const users = await user.find(query, { password: 0 })
      .sort({ created_date: -1 }) // 按创建日期降序排列
      .skip(skip)
      .limit(limit)
      .lean();
    
    // 获取所有角色信息以便映射
    const allRoles = await role.find({});
    const roleMap = new Map();
    allRoles.forEach(r => {
      roleMap.set(r._id, r.role_type);
    });
    
    // 为每个用户添加角色信息
    const usersWithRole = users.map(u => ({
      ...u,
      role: roleMap.get(u.role_id) || 'Unknown'
    }));
    
    // 计算总页数
    const totalPages = Math.ceil(totalUsers / limit);
    
    logger.info(`Admin users list accessed by: ${req.username}, page: ${page}, limit: ${limit}`);
    
    return res.status(200).json({
      status: true,
      message: "Users retrieved successfully",
      data: {
        users: usersWithRole,
        pagination: {
          currentPage: page,
          totalPages: totalPages,
          totalUsers: totalUsers,
          limit: limit,
          hasNextPage: page < totalPages,
          hasPrevPage: page > 1
        }
      }
    });
  }
  catch(err) {
    logger.error("Admin users list error: " + err.message);
    res.status(500).json({
      status: false,
      message: "Internal server error",
      error: err.message
    });
  }
});

// 管理员 - 获取单个客户端用户详情（用于编辑）
const DEFAULT_CLIENT_AVATAR = 'https://gw.alipayobjects.com/zos/rmsportal/BiazfanxmamNRoxxVxka.png';
const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

router.post('/admin/users', admin_checking, async function(req, res, next) {
  try {
    const {
      username,
      password,
      email,
      phone,
      gender,
      avatar,
      role_id
    } = req.body || {};

    const trimmedUsername = typeof username === 'string' ? username.trim() : '';
    const rawPassword = typeof password === 'string' ? password : '';
    const trimmedEmail = typeof email === 'string' ? email.trim().toLowerCase() : '';
    const trimmedGender = typeof gender === 'string' ? gender.trim() : '';
    const trimmedAvatar = typeof avatar === 'string' && avatar.trim() !== '' ? avatar.trim() : DEFAULT_CLIENT_AVATAR;
    const normalizedPhone = typeof phone === 'string' ? phone.replace(/\s+/g, '') : '';

    if (!trimmedUsername || !rawPassword || !trimmedEmail || !normalizedPhone || !trimmedGender) {
      return res.status(400).json({
        status: false,
        message: 'username, password, email, phone and gender are required'
      });
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(trimmedEmail)) {
      return res.status(400).json({
        status: false,
        message: 'Invalid email format'
      });
    }

    if (normalizedPhone.length < 8) {
      return res.status(400).json({
        status: false,
        message: 'Invalid phone number'
      });
    }

    if (rawPassword.length < 6) {
      return res.status(400).json({
        status: false,
        message: 'Password must be at least 6 characters'
      });
    }

    if (typeof role_id === 'undefined') {
      return res.status(400).json({
        status: false,
        message: 'role_id is required'
      });
    }

    const parsedRoleId = Number(role_id);
    if (!Number.isInteger(parsedRoleId)) {
      return res.status(400).json({
        status: false,
        message: 'role_id must be an integer'
      });
    }

    const roleDetail = await role.findOne({ _id: parsedRoleId }).lean();

    if (!roleDetail) {
      return res.status(400).json({
        status: false,
        message: 'Role not found'
      });
    }

    if (roleDetail.role_type === 'Admin') {
      return res.status(403).json({
        status: false,
        message: 'Creating admin users is not allowed'
      });
    }

    const existingUser = await user.findOne({
      $or: [
        { username: trimmedUsername },
        { email: trimmedEmail },
        { phone: normalizedPhone }
      ]
    }).lean();

    if (existingUser) {
      let conflictField = 'username';
      if (existingUser.email === trimmedEmail) {
        conflictField = 'email';
      } else if (existingUser.phone === normalizedPhone) {
        conflictField = 'phone';
      }

      return res.status(409).json({
        status: false,
        message: `This ${conflictField} already exists`
      });
    }

    const hashedPassword = bcrypt.hashSync(rawPassword, 8);
    const now = DateTime.now().toISO();

    const newUser = new user({
      username: trimmedUsername,
      password: hashedPassword,
      gender: trimmedGender,
      email: trimmedEmail,
      phone: normalizedPhone,
      avatar: trimmedAvatar,
      created_date: now,
      last_active: now,
      last_action: 'Created by admin',
      is_checking: false,
      role_id: parsedRoleId
    });

    await newUser.save();

    const createdUser = await user.findOne({ _id: newUser._id }, { password: 0 }).lean();

    logger.info(`Admin created user: ${trimmedUsername} (${newUser._id}) by ${req.username}`);

    return res.status(201).json({
      status: true,
      message: 'User created successfully',
      data: {
        user: createdUser
      }
    });
  } catch (err) {
    logger.error('Admin create user error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

router.get('/admin/users/:id/edit', admin_checking, async function(req, res, next) {
  try {
    const userId = Number(req.params.id);

    if (!Number.isInteger(userId)) {
      return res.status(400).json({
        status: false,
        message: 'Invalid user ID'
      });
    }

    // 查找目标用户（排除密码字段）
    const userDetail = await user.findOne({ _id: userId }, { password: 0 }).lean();

    if (!userDetail) {
      return res.status(404).json({
        status: false,
        message: 'User not found'
      });
    }

    // 校验：不允许对管理员用户执行编辑接口
    const adminRoles = await role.find({ role_type: 'Admin' });
    const adminRoleIds = adminRoles.map(r => r._id);

    if (adminRoleIds.includes(userDetail.role_id)) {
      return res.status(403).json({
        status: false,
        message: 'Editing admin users is not allowed'
      });
    }

    const userRole = await role.findById(userDetail.role_id).lean();

    logger.info(`Admin fetched user detail for edit: ${userDetail.username} (${userId}) by ${req.username}`);

    return res.status(200).json({
      status: true,
      message: 'User detail retrieved successfully',
      data: {
        user: {
          ...userDetail,
          role: userRole ? userRole.role_type : 'Unknown'
        }
      }
    });
  } catch (err) {
    logger.error('Admin user detail error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 更新客户端用户信息
router.post('/admin/users/:id', admin_checking, async function(req, res, next) {
  try {
    const userId = Number(req.params.id);

    if (!Number.isInteger(userId)) {
      return res.status(400).json({
        status: false,
        message: 'Invalid user ID'
      });
    }

    const allowedFields = ['username', 'email', 'phone', 'gender', 'avatar', 'is_checking', 'role_id'];
    const updates: any = {};

    allowedFields.forEach((field) => {
      if (typeof req.body[field] !== 'undefined') {
        if (typeof req.body[field] === 'string') {
          updates[field] = req.body[field].trim();
        } else {
          updates[field] = req.body[field];
        }
      }
    });

    if (Object.keys(updates).length === 0) {
      return res.status(400).json({
        status: false,
        message: 'No valid fields provided for update'
      });
    }

    const targetUser = await user.findOne({ _id: userId }).lean();

    if (!targetUser) {
      return res.status(404).json({
        status: false,
        message: 'User not found'
      });
    }

    const adminRoles = await role.find({ role_type: 'Admin' });
    const adminRoleIds = adminRoles.map(r => r._id);

    if (adminRoleIds.includes(targetUser.role_id)) {
      return res.status(403).json({
        status: false,
        message: 'Updating admin users is not allowed'
      });
    }

    if (typeof updates.is_checking !== 'undefined') {
      if (typeof updates.is_checking !== 'boolean') {
        return res.status(400).json({
          status: false,
          message: 'is_checking must be a boolean value'
        });
      }
    }

    if (updates.email) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(updates.email)) {
        return res.status(400).json({
          status: false,
          message: 'Invalid email format'
        });
      }
    }

    if (updates.phone) {
      updates.phone = updates.phone.replace(/\s+/g, '');
      if (updates.phone.length < 8) {
        return res.status(400).json({
          status: false,
          message: 'Invalid phone number'
        });
      }
    }

    if (typeof updates.role_id !== 'undefined') {
      const parsedRoleId = Number(updates.role_id);
      if (!Number.isInteger(parsedRoleId)) {
        return res.status(400).json({
          status: false,
          message: 'role_id must be an integer'
        });
      }

      const roleDetail = await role.findOne({ _id: parsedRoleId }).lean();

      if (!roleDetail) {
        return res.status(400).json({
          status: false,
          message: 'Role not found'
        });
      }

      if (roleDetail.role_type === 'Admin') {
        return res.status(403).json({
          status: false,
          message: 'Assigning admin role is not allowed'
        });
      }

      updates.role_id = parsedRoleId;
    }

    await user.updateOne({ _id: userId }, { $set: updates });

    const updatedUser = await user.findOne({ _id: userId }, { password: 0 }).lean();

    logger.info(`Admin updated user: ${updatedUser?.username} (${userId}) by ${req.username}`);

    return res.status(200).json({
      status: true,
      message: 'User updated successfully',
      data: {
        user: updatedUser
      }
    });
  } catch (err) {
    logger.error('Admin update user error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

router.get('/admin/users/:id/delete', admin_checking, async function(req, res, next) {
  try {
    const userId = Number(req.params.id);

    if (!Number.isInteger(userId)) {
      return res.status(400).json({
        status: false,
        message: 'Invalid user ID'
      });
    }

    const userDetail = await user.findOne({ _id: userId }).lean();

    if (!userDetail) {
      return res.status(404).json({
        status: false,
        message: 'User not found'
      });
    }

    const adminRoles = await role.find({ role_type: 'Admin' });
    const adminRoleIds = adminRoles.map(r => r._id);

    if (adminRoleIds.includes(userDetail.role_id)) {
      return res.status(403).json({
        status: false,
        message: 'Deleting admin users is not allowed'
      });
    }

    await user.deleteOne({ _id: userId });

    logger.info(`Admin deleted user: ${userDetail.username} (${userId}) by ${req.username}`);

    return res.status(200).json({
      status: true,
      message: 'User deleted successfully'
    });
  } catch (err) {
    logger.error('Admin delete user error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 分类列表
router.get('/admin/categories', admin_checking, async function(req, res, next) {
  try {
    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 10;
    const search = typeof req.query.search === 'string' ? req.query.search.trim() : '';
    const skip = (page - 1) * limit;

    const query: Record<string, any> = {};
    if (search) {
      query.category_name = { $regex: escapeRegExp(search), $options: 'i' };
    }

    const totalCategories = await category.countDocuments(query);

    const categories = await category.find(query)
      .sort({ _id: -1 })
      .skip(skip)
      .limit(limit)
      .lean();

    logger.info(`Admin categories list accessed by: ${req.username}, page: ${page}, limit: ${limit}`);

    return res.status(200).json({
      status: true,
      message: 'Categories retrieved successfully',
      data: {
        categories,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(totalCategories / limit),
          totalCategories,
          limit,
          hasNextPage: page * limit < totalCategories,
          hasPrevPage: page > 1
        }
      }
    });
  } catch (err) {
    logger.error('Admin categories list error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 创建分类
router.post('/admin/categories', admin_checking, async function(req, res, next) {
  try {
    const { category_name } = req.body || {};
    
    const trimmedName = typeof category_name === 'string' ? category_name.trim() : '';

    if (!trimmedName) {
      return res.status(400).json({
        status: false,
        message: 'category_name is required'
      });
    }

    const nameRegex = new RegExp(`^${escapeRegExp(trimmedName)}$`, 'i');
    const existingCategory = await category.findOne({ category_name: nameRegex }).lean();

    if (existingCategory) {
      return res.status(409).json({
        status: false,
        message: 'Category name already exists'
      });
    }

    const nowIso = new Date().toISOString();
    const newCategory = new category({
      category_name: trimmedName,
      created_date: nowIso,
      updated_date: nowIso
    });

    await newCategory.save();

    logger.info(`Admin created category: ${trimmedName} (${newCategory._id}) by ${req.username}`);

    return res.status(201).json({
      status: true,
      message: 'Category created successfully',
      data: {
        category: newCategory
      }
    });
  } catch (err) {
    logger.error('Admin create category error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 获取分类详情
router.get('/admin/categories/:id', admin_checking, async function(req, res, next) {
  try {
    const categoryId = Number(req.params.id);

    if (!Number.isInteger(categoryId)) {
      return res.status(400).json({
        status: false,
        message: 'Invalid category ID'
      });
    }

    const categoryDetail = await category.findOne({ _id: categoryId }).lean();

    if (!categoryDetail) {
      return res.status(404).json({
        status: false,
        message: 'Category not found'
      });
    }

    logger.info(`Admin fetched category detail: ${categoryDetail.category_name} (${categoryId}) by ${req.username}`);

    return res.status(200).json({
      status: true,
      message: 'Category detail retrieved successfully',
      data: {
        category: categoryDetail
      }
    });
  } catch (err) {
    logger.error('Admin category detail error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 更新分类（使用PUT）
router.post('/admin/categories/:id', admin_checking, async function(req, res, next) {
  try {
    const categoryId = Number(req.params.id);

    if (!Number.isInteger(categoryId)) {
      return res.status(400).json({
        status: false,
        message: 'Invalid category ID'
      });
    }

    const categoryDetail = await category.findOne({ _id: categoryId }).lean();

    if (!categoryDetail) {
      return res.status(404).json({
        status: false,
        message: 'Category not found'
      });
    }

    const { category_name } = req.body || {};
    const trimmedName = typeof category_name === 'string' ? category_name.trim() : '';

    if (!trimmedName) {
      return res.status(400).json({
        status: false,
        message: 'category_name is required'
      });
    }

    const duplicate = await category.findOne({
      _id: { $ne: categoryId },
      category_name: new RegExp(`^${escapeRegExp(trimmedName)}$`, 'i')
    }).lean();

    if (duplicate) {
      return res.status(409).json({
        status: false,
        message: 'Category name already exists'
      });
    }

    const nowIso = new Date().toISOString();

    await category.updateOne(
      { _id: categoryId },
      {
        $set: {
          category_name: trimmedName,
          updated_date: nowIso
        }
      }
    );

    const updatedCategory = await category.findOne({ _id: categoryId }).lean();

    logger.info(`Admin updated category: ${updatedCategory?.category_name} (${categoryId}) by ${req.username}`);

    return res.status(200).json({
      status: true,
      message: 'Category updated successfully',
      data: {
        category: updatedCategory
      }
    });
  } catch (err) {
    logger.error('Admin update category error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 删除分类（使用GET）
router.get('/admin/categories/:id/delete', admin_checking, async function(req, res, next) {
  try {
    const categoryId = Number(req.params.id);

    if (!Number.isInteger(categoryId)) {
      return res.status(400).json({
        status: false,
        message: 'Invalid category ID'
      });
    }

    const categoryDetail = await category.findOne({ _id: categoryId }).lean();

    if (!categoryDetail) {
      return res.status(404).json({
        status: false,
        message: 'Category not found'
      });
    }

    const relatedProducts = await rubik_info.countDocuments({ category_id: categoryId });

    if (relatedProducts > 0) {
      return res.status(409).json({
        status: false,
        message: 'Cannot delete category with associated products'
      });
    }

    await category.deleteOne({ _id: categoryId });

    logger.info(`Admin deleted category: ${categoryDetail.category_name} (${categoryId}) by ${req.username}`);

    return res.status(200).json({
      status: true,
      message: 'Category deleted successfully'
    });
  } catch (err) {
    logger.error('Admin delete category error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 魔方类型列表
router.get('/admin/rubik-types', admin_checking, async function(req, res, next) {
  try {
    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 10;
    const search = typeof req.query.search === 'string' ? req.query.search.trim() : '';
    const skip = (page - 1) * limit;

    const query: Record<string, any> = {};
    if (search) {
      query.type_name = { $regex: escapeRegExp(search), $options: 'i' };
    }

    const totalRubikTypes = await rubikType.countDocuments(query);
  
    const rubikTypes = await rubikType.find(query)
      .sort({ _id: -1 })
      .skip(skip)
      .limit(limit)
      .lean();

    

    logger.info(`Admin rubik types list accessed by: ${req.username}, page: ${page}, limit: ${limit}`);

    console.log("Rubik type retrieved successfully:"+JSON.stringify(rubikTypes));


    return res.status(200).json({
      status: true,
      message: 'Rubik types retrieved successfully',
      data: {
        rubikTypes,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(totalRubikTypes / limit),
          totalRubikTypes,
          limit,
          hasNextPage: page * limit < totalRubikTypes,
          hasPrevPage: page > 1
        }
      }
    });
  } catch (err) {
    logger.error('Admin rubik types list error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 创建魔方类型
router.post('/admin/rubik-types', admin_checking, async function(req, res, next) {
  try {
    const { type_name, variation } = req.body || {};
    const trimmedName = typeof type_name === 'string' ? type_name.trim() : '';
    const hasVariation = typeof variation !== 'undefined' && variation !== null && variation !== '';
    const parsedVariation = hasVariation ? Number(variation) : NaN;    

    if (!trimmedName || !hasVariation || Number.isNaN(parsedVariation)) {
      return res.status(400).json({
        status: false,
        message: 'type_name and numeric variation are required'
      });
    }

    const duplicate = await rubikType.findOne({
      type_name: new RegExp(`^${escapeRegExp(trimmedName)}$`, 'i')
    }).lean();

    if (duplicate) 
    {
      return res.status(409).json({
        status: false,
        message: 'Rubik type name already exists'
      });      
    }

    const nowIso = new Date().toISOString();
    const newRubikType = new rubikType({
      type_name: trimmedName,
      variation: parsedVariation,
      created_date: nowIso,
      updated_date: nowIso
    });

    await newRubikType.save();

    logger.info(`Admin created rubik type: ${trimmedName} (${newRubikType._id}) by ${req.username}`);    

    return res.status(201).json({
      status: true,
      message: 'Rubik type created successfully',
      data: {
        rubikType: newRubikType
      }
    });
  } catch (err) {
    logger.error('Admin create rubik type error: ' + err.message);

    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 获取魔方类型详情
router.get('/admin/rubik-types/:id', admin_checking, async function(req, res, next) {
  try {

    const rubikTypeId = Number(req.params.id);
  
    if (!Number.isInteger(rubikTypeId)) {
      return res.status(400).json({
        status: false,
        message: 'Invalid rubik type ID'
      });      
    }

    const rubikTypeDetail = await rubikType.findOne({ _id: rubikTypeId }).lean();
    
    if (!rubikTypeDetail) {
      return res.status(404).json({
        status: false,
        message: 'Rubik type not found'
      });
    }

    logger.info(`Admin fetched rubik type detail: ${rubikTypeDetail.type_name} (${rubikTypeId}) by ${req.username}`);

    return res.status(200).json({
      status: true,
      message: 'Rubik type detail retrieved successfully',
      data: {
        rubikType: rubikTypeDetail
      }
    });
  } catch (err) {
    logger.error('Admin rubik type detail error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 更新魔方类型（使用POST）
router.post('/admin/rubik-types/:id', admin_checking, async function(req, res, next) {
  try {
    const rubikTypeId = Number(req.params.id);

    if (!Number.isInteger(rubikTypeId)) {
      return res.status(400).json({
        status: false,
        message: 'Invalid rubik type ID'
      });
    }

    const rubikTypeDetail = await rubikType.findOne({ _id: rubikTypeId }).lean();

    if (!rubikTypeDetail) {
      return res.status(404).json({
        status: false,
        message: 'Rubik type not found'
      });
    }

    const { type_name, variation } = req.body || {};
    const updates: Record<string, any> = {};

    if (typeof type_name !== 'undefined') {
      const trimmedName = typeof type_name === 'string' ? type_name.trim() : '';

      if (!trimmedName) {
        return res.status(400).json({
          status: false,
          message: 'type_name is required when provided'
        });
      }

      const duplicate = await rubikType.findOne({
        _id: { $ne: rubikTypeId },
        type_name: new RegExp(`^${escapeRegExp(trimmedName)}$`, 'i')
      }).lean();

      if (duplicate) {
        return res.status(409).json({
          status: false,
          message: 'Rubik type name already exists'
        });
      }

      updates.type_name = trimmedName;
    }

    if (typeof variation !== 'undefined') {
      if (variation === null || variation === '') {
        return res.status(400).json({
          status: false,
          message: 'variation must be provided when specified'
        });
      }

      const parsedVariation = Number(variation);

      if (Number.isNaN(parsedVariation)) {
        return res.status(400).json({
          status: false,
          message: 'variation must be a number'
        });
      }

      updates.variation = parsedVariation;
    }

    if (Object.keys(updates).length === 0) {
      return res.status(400).json({
        status: false,
        message: 'No fields provided for update'
      });
    }

    updates.updated_date = new Date().toISOString();

    await rubikType.updateOne(
      { _id: rubikTypeId },
      { $set: updates }
    );

    const updatedRubikType = await rubikType.findOne({ _id: rubikTypeId }).lean();

    logger.info(`Admin updated rubik type: ${updatedRubikType?.type_name} (${rubikTypeId}) by ${req.username}`);

    return res.status(200).json({
      status: true,
      message: 'Rubik type updated successfully',
      data: {
        rubikType: updatedRubikType
      }
    });
  } catch (err) {
    logger.error('Admin update rubik type error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 删除魔方类型（使用GET）
router.get('/admin/rubik-types/:id/delete', admin_checking, async function(req, res, next) {
  try {
    const rubikTypeId = Number(req.params.id);

    if (!Number.isInteger(rubikTypeId)) {
      return res.status(400).json({
        status: false,
        message: 'Invalid rubik type ID'
      });
    }

    const rubikTypeDetail = await rubikType.findOne({ _id: rubikTypeId }).lean();

    if (!rubikTypeDetail) {
      return res.status(404).json({
        status: false,
        message: 'Rubik type not found'
      });
    }

    await rubikType.deleteOne({ _id: rubikTypeId });

    logger.info(`Admin deleted rubik type: ${rubikTypeDetail.type_name} (${rubikTypeId}) by ${req.username}`);
  
    return res.status(200).json({
      status: true,
      message: 'Rubik type deleted successfully'
    });
  } catch (err) {
    logger.error('Admin delete rubik type error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 产品列表
router.get('/admin/products', admin_checking, async function(req, res, next) {
  try {
    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 10;
    const search = (req.query.search as string) || '';
    const skip = (page - 1) * limit;

    const query: any = {};
    if (search) {
      query.$or = [
        { name: { $regex: search, $options: 'i' } },
        { description: { $regex: search, $options: 'i' } }
      ];
    }

    const totalProducts = await rubik_info.countDocuments(query);
    const products = await rubik_info.find(query)
      .sort({ _id: -1 })
      .skip(skip)
      .limit(limit)
      .lean();

    logger.info(`Admin products list accessed by: ${req.username}, page: ${page}, limit: ${limit}`);

    return res.status(200).json({
      status: true,
      message: 'Products retrieved successfully',
      data: {
        products,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(totalProducts / limit),
          totalProducts,
          limit,
          hasNextPage: page * limit < totalProducts,
          hasPrevPage: page > 1
        }
      }
    });
  } catch (err) {
    logger.error('Admin products list error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

router.post('/admin/products', admin_checking, async function(req, res, next) {
  try {
    const {
      name,
      description,
      avatar,
      feature,
      category_id
    } = req.body || {};

    const trimmedName = typeof name === 'string' ? name.trim() : '';
    const trimmedDescription = typeof description === 'string' ? description.trim() : '';
    const trimmedAvatar = typeof avatar === 'string' ? avatar.trim() : '';
    const trimmedFeature = typeof feature === 'string' ? feature.trim() : '';
    const hasCategory = typeof category_id !== 'undefined' && category_id !== null && category_id !== '';

    if (!trimmedName || !trimmedDescription || !trimmedAvatar || !trimmedFeature) {
      return res.status(400).json({
        status: false,
        message: 'name, description, avatar and feature are required'
      });
    }

    const nameRegex = new RegExp(`^${escapeRegExp(trimmedName)}$`, 'i');
    const existingProduct = await rubik_info.findOne({ name: nameRegex }).lean();

    if (existingProduct) {
      return res.status(409).json({
        status: false,
        message: 'Product name already exists'
      });
    }

    let parsedCategoryId: number | undefined = undefined;
    if (hasCategory) {
      parsedCategoryId = Number(category_id);
      if (!Number.isInteger(parsedCategoryId)) {
        return res.status(400).json({
          status: false,
          message: 'category_id must be an integer'
        });
      }
    }

    const productPayload: any = {
      name: trimmedName,
      description: trimmedDescription,
      avatar: trimmedAvatar,
      feature: trimmedFeature
    };

    if (typeof parsedCategoryId !== 'undefined') {
      productPayload.category_id = parsedCategoryId;
    }

    const product = new rubik_info(productPayload);
    await product.save();

    logger.info(`Admin created product: ${trimmedName} (${product._id}) by ${req.username}`);

    return res.status(201).json({
      status: true,
      message: 'Product created successfully',
      data: {
        product
      }
    });
  } catch (err) {
    logger.error('Admin create product error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 获取单个产品详情
router.get('/admin/products/:id/edit', admin_checking, async function(req, res, next) {
  try {
    const productId = req.params.id;

    if (!productId || typeof productId !== 'string' || productId.trim() === '') {
      return res.status(400).json({
        status: false,
        message: 'Invalid product ID'
      });
    }

    const productDetail = await rubik_info.findOne({ _id: productId.trim() }).lean();

    if (!productDetail) {
    console.log("Product not found");
      return res.status(404).json({
        status: false,
        message: 'Product not found'
      });
    }

    // Fetch category information so the edit page can show and change category
    const categories = await category.find({}).lean();

    logger.info(`Admin fetched product detail: ${productDetail.name} (${productId.trim()}) by ${req.username}`);

    return res.status(200).json({
      status: true,
      message: 'Product detail retrieved successfully',
      data: {
        product: productDetail,
        categories: categories
      }
    });
  } catch (err) {
    logger.error('Admin product detail error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 更新产品信息（使用POST）
router.post('/admin/products/:id', admin_checking, async function(req, res, next) {
  try {

    const productId = req.params.id;

    if (!productId || typeof productId !== 'string' || productId.trim() === '') {
      return res.status(400).json({
        status: false,
        message: 'Invalid product ID'
      });
    }

    const trimmedProductId = productId.trim();

    const allowedFields = ['name', 'description', 'avatar', 'feature', 'category_id'];
    
    const updates: Record<string, any> = {};

    allowedFields.forEach((field) => {
      if (typeof req.body[field] !== 'undefined') {
        if (typeof req.body[field] === 'string') {
          updates[field] = req.body[field].trim();
        } else {
          updates[field] = req.body[field];
        }
      }
    });

    if (Object.keys(updates).length === 0) {
      return res.status(400).json({
        status: false,
        message: 'No valid fields provided for update'
      });
    }

    const productDetail = await rubik_info.findOne({ _id: trimmedProductId }).lean();

    if (!productDetail) {
      return res.status(404).json({
        status: false,
        message: 'Product not found'
      });
    }

    if (typeof updates.name !== 'undefined') {
      if (!updates.name) {
        return res.status(400).json({
          status: false,
          message: 'name cannot be empty'
        });
      }

      const nameRegex = new RegExp(`^${escapeRegExp(updates.name)}$`, 'i');
      const duplicate = await rubik_info.findOne({
        _id: { $ne: trimmedProductId },
        name: nameRegex
      }).lean();

      if (duplicate) {
        return res.status(409).json({
          status: false,
          message: 'Product name already exists'
        });
      }
    }

    // Handle category update when provided
    if (typeof updates.category_id !== 'undefined') {
      const parsedCategoryId = Number(updates.category_id);

      // Validate category_id is an integer
      if (!Number.isInteger(parsedCategoryId)) {
        return res.status(400).json({
          status: false,
          message: 'category_id must be an integer'
        });
      }

      // Ensure the category exists before assigning
      const categoryDetail = await category.findOne({ _id: parsedCategoryId }).lean();
      if (!categoryDetail) {
        return res.status(400).json({
          status: false,
          message: 'Category not found'
        });
      }

      updates.category_id = parsedCategoryId;
    }

    await rubik_info.updateOne({ _id: trimmedProductId }, { $set: updates });

    const updatedProduct = await rubik_info.findOne({ _id: trimmedProductId }).lean();

    logger.info(`Admin updated product: ${updatedProduct?.name} (${trimmedProductId}) by ${req.username}`);

    return res.status(200).json({
      status: true,
      message: 'Product updated successfully',
      data: {
        product: updatedProduct
      }
    });
  } catch (err) {
    logger.error('Admin update product error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 删除产品（使用GET）
router.get('/admin/products/:id/delete', admin_checking, async function(req, res, next) {
  try {
    const productId = req.params.id;

    if (!productId || typeof productId !== 'string' || productId.trim() === '') {
      return res.status(400).json({
        status: false,
        message: 'Invalid product ID'
      });      
    }

    const trimmedProductId = productId.trim();

    const productDetail = await rubik_info.findOne({ _id: trimmedProductId }).lean();

    if (!productDetail) {
      return res.status(404).json({
        status: false,
        message: 'Product not found'
      });
    }

    await rubik_info.deleteOne({ _id: trimmedProductId });

    logger.info(`Admin deleted product: ${productDetail.name} (${trimmedProductId}) by ${req.username}`);

    return res.status(200).json({
      status: true,
      message: 'Product deleted successfully'
    });
  } catch (err) {
    logger.error('Admin delete product error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});


// 用户 - 创建反馈
router.post('/feedback', user_only_checking, async function(req, res, next) {
  try {
    const { feedback_content, feedback_response } = req.body || {};
    const trimmedContent = typeof feedback_content === 'string' ? feedback_content.trim() : '';

    if (!trimmedContent) {
      return res.status(400).json({
        status: false,
        message: 'feedback_content is required'
      });
    }

    let resolvedUserId: number | null = null;

    if (typeof req.userId !== 'undefined' && req.userId !== null) {
      const parsedUserId = Number(req.userId);
      if (!Number.isNaN(parsedUserId) && Number.isInteger(parsedUserId)) {
        resolvedUserId = parsedUserId;
      }
    }

    if (resolvedUserId === null && req.username) {
      const userInfo = await user.findOne({ username: req.username }).lean();
      if (userInfo && typeof userInfo._id !== 'undefined') {
        const parsedUserId = Number(userInfo._id);
        if (!Number.isNaN(parsedUserId) && Number.isInteger(parsedUserId)) {
          resolvedUserId = parsedUserId;
        }
      }
    }

    if (resolvedUserId === null) {
      return res.status(401).json({
        status: false,
        message: 'Unable to resolve user id from session'
      });
    }

    const now = DateTime.now().toISO();
    const payload: Record<string, any> = {
      user_id: resolvedUserId,
      feedback_content: trimmedContent,
      feedback_response: '',
      created_date: now,
      updated_date: now
    };

    const trimmedResponse = typeof feedback_response === 'string' ? feedback_response.trim() : '';

    if (trimmedResponse) {
      payload.feedback_response = trimmedResponse;
    }

    const feedbackItem = new feedback(payload);
    await feedbackItem.save();

    logger.info(`User ${req.username || resolvedUserId} created feedback: ${feedbackItem._id}`);

    return res.status(201).json({
      status: true,
      message: 'Feedback submitted successfully',
      data: {
        feedback: feedbackItem
      }
    });
  } catch (err) {
    logger.error('Create feedback error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 获取反馈列表
router.get('/admin/feedback', admin_checking, async function(req, res, next) {
  try {
    const page = parseInt(req.query.page as string) || 1;
    const limit = parseInt(req.query.limit as string) || 10;
    const search = (req.query.search as string) || '';
    const skip = (page - 1) * limit;

    const query: Record<string, any> = {};

    if (search) {
      const escaped = escapeRegExp(search.trim());
      query.$or = [
        { feedback_content: { $regex: escaped, $options: 'i' } },
        { feedback_response: { $regex: escaped, $options: 'i' } }
      ];
    }

    const totalFeedback = await feedback.countDocuments(query);
    const feedbackList = await feedback.find(query)
      .sort({ _id: -1 })
      .skip(skip)
      .limit(limit)
      .lean();

    const userIds = Array.from(
      new Set(
        feedbackList
          .map((item: any) => Number(item.user_id))
          .filter((id: number) => Number.isInteger(id))
      )
    );

    let userMap = new Map<number, any>();

    if (userIds.length > 0) {
      const usersInfo = await user
        .find({ _id: { $in: userIds } }, { _id: 1, username: 1, display_name: 1, email: 1 })
        .lean();

      userMap = new Map(usersInfo.map((info: any) => [info._id, info]));
    }

    const feedbackWithUser = feedbackList.map((item: any) => ({
      ...item,
      user: userMap.get(Number(item.user_id)) || null
    }));

    logger.info(`Admin ${req.username} fetched feedback list - page ${page}, limit ${limit}`);

    return res.status(200).json({
      status: true,
      message: 'Feedback list retrieved successfully',
      data: {
        feedback: feedbackWithUser,
        pagination: {
          currentPage: page,
          totalPages: Math.ceil(totalFeedback / limit) || 1,
          totalFeedback,
          limit,
          hasNextPage: page * limit < totalFeedback,
          hasPrevPage: page > 1
        }
      }
    });
  } catch (err) {
    logger.error('Admin feedback list error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 获取单条反馈详情
router.get('/admin/feedback/:id', admin_checking, async function(req, res, next) {
  try {
    const feedbackId = Number(req.params.id);

    if (!Number.isInteger(feedbackId)) {
      return res.status(400).json({
        status: false,
        message: 'Invalid feedback ID'
      });
    }

    const feedbackDetail = await feedback.findOne({ _id: feedbackId }).lean();

    if (!feedbackDetail) {
      return res.status(404).json({
        status: false,
        message: 'Feedback not found'
      });
    }

    let feedbackOwner: any = null;

    if (Number.isInteger(Number(feedbackDetail.user_id))) {
      feedbackOwner = await user
        .findOne(
          { _id: Number(feedbackDetail.user_id) },
          { _id: 1, username: 1, display_name: 1, email: 1 }
        )
        .lean();
    }

    logger.info(`Admin ${req.username} fetched feedback detail: ${feedbackId}`);

    return res.status(200).json({
      status: true,
      message: 'Feedback detail retrieved successfully',
      data: {
        feedback: feedbackDetail,
        user: feedbackOwner
      }
    });
  } catch (err) {
    logger.error('Admin feedback detail error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 更新反馈（使用POST）
router.post('/admin/feedback/:id', admin_checking, async function(req, res, next) {
  try {
    const feedbackId = Number(req.params.id);

    if (!Number.isInteger(feedbackId)) {
      return res.status(400).json({
        status: false,
        message: 'Invalid feedback ID'
      });
    }

    const allowedFields = ['feedback_content', 'feedback_response'];
    const updates: Record<string, any> = {};

    allowedFields.forEach((field) => {
      if (req.body && typeof req.body[field] !== 'undefined') {
        if (typeof req.body[field] === 'string') {
          updates[field] = req.body[field].trim();
        } else {
          updates[field] = req.body[field];
        }
      }
    });

    if (Object.keys(updates).length === 0) {
      return res.status(400).json({
        status: false,
        message: 'No valid fields provided for update'
      });
    }

    if (typeof updates.feedback_content !== 'undefined' && !updates.feedback_content) {
      return res.status(400).json({
        status: false,
        message: 'feedback_content cannot be empty'
      });
    }

    const feedbackDetail = await feedback.findOne({ _id: feedbackId }).lean();

    if (!feedbackDetail) {
      return res.status(404).json({
        status: false,
        message: 'Feedback not found'
      });
    }

    updates.updated_date = DateTime.now().toISO();

    await feedback.updateOne({ _id: feedbackId }, { $set: updates });
    const updatedFeedback = await feedback.findOne({ _id: feedbackId }).lean();

    logger.info(`Admin ${req.username} updated feedback: ${feedbackId}`);

    return res.status(200).json({
      status: true,
      message: 'Feedback updated successfully',
      data: {
        feedback: updatedFeedback
      }
    });
  } catch (err) {
    logger.error('Admin update feedback error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});

// 管理员 - 删除反馈（使用GET）
router.get('/admin/feedback/:id/delete', admin_checking, async function(req, res, next) {
  try {
    const feedbackId = Number(req.params.id);

    if (!Number.isInteger(feedbackId)) {
      return res.status(400).json({
        status: false,
        message: 'Invalid feedback ID'
      });
    }

    const feedbackDetail = await feedback.findOne({ _id: feedbackId }).lean();

    if (!feedbackDetail) {
      return res.status(404).json({
        status: false,
        message: 'Feedback not found'
      });
    }

    await feedback.deleteOne({ _id: feedbackId });

    logger.info(`Admin ${req.username} deleted feedback: ${feedbackId}`);

    return res.status(200).json({
      status: true,
      message: 'Feedback deleted successfully'
    });
  } catch (err) {
    logger.error('Admin delete feedback error: ' + err.message);
    return res.status(500).json({
      status: false,
      message: 'Internal server error',
      error: err.message
    });
  }
});


router.post('/forgot_password',async function(req,res,next){
   try
   {
    var phone = req.body.phone.trim();
    var qr_code = req.body.qr;
    console.log(phone);

    var otp=convertQrToOtp(qr_code);

    console.log('Your otp is:'+otp);

    const sent_data={
      messages:[
        {
          destinations:[
            {
            to:phone
            }
          ],
         from:'InfoSMS', 
         text:`Your verify OTP is ${otp}`
        }
      ]
    };
  
   const headers={
    'Authorization':`App ${API_KEY}`,
    'Content-Type':'application/json',
    'Accept':'application/json'
   };
   const config={headers:headers};
   const url=`${INFOBIP_API_BASE_URL}sms/2/text/advanced`
   const response=await axios.post(url,sent_data,config).then((mess)=>
   { 
    logger.info(`Forgot Password:Sent OTP ${otp} successfully:${JSON.stringify(mess.data)}`);
    res.status(200).send({status:true,message:JSON.stringify(mess.data)});
   }).catch((err)=>{
   logger.error(`Sent OTP ${otp} failed:${err.message}`);    
   res.status(400).send({status:false,message:err.message});
   });
    // twilio_client.messages.create({
    //   body:`Your verify OTP is ${otp}`,
    //   from:twilio_phone,
    //   to:'+84906744816'
    // }).then((mess)=>{
    //   console.log('Sent Message Successfully');
    //   logger.info(`Forgot Password:Sent OTP ${otp} successfully:${mess}`);
    //   res.status(200).send({status:true,message:`Sent message to ${phone} successfully`});
    // }).catch(err=>
    // {
    //   logger.error(`Sent OTP ${otp} failed:${err.message}`);
    //   res.status(400).send({status:false,message:err.message});
    // })
   }
   catch(ex)
   {
    logger.error("Forget Password Exception:"+ex.message);
   } 
});


router.post('/reset-password',async function(req,res,next){
try
{
  var phone = req.body.phone;

  if(phone.indexOf('0')==0)
    {
      phone=phone.replace('0','+84');
    }

  var is_valid_phone=checkValidPhone(phone);

  if(!is_valid_phone)
    { 
      logger.info('Phone Number is invalid');
      res.status(400).send({status:false,message:'Phone Number is invalid'});
      return;
    }

  var password = req.body.password;
  
  if(!checkPassword(password))
    {
      res.status(400).send({status:false,message:'Your password is not strong enough.'})
      return;
    }
  
  const new_password=bcrypt.hashSync(password,8);
  
  var user_found=await user.findOne({phone:phone});

  if(user_found)
    {
    var response=await user.updateOne({phone:user_found.phone},{$set:{password:new_password}});
    logger.info("Reset password successfully");
    res.status(200).send({status:true,message:'Reset password successfully'});
    return;
    }
    else
    {
      logger.error("User not exist");
      res.status(400).send({status:false,message:'User not exist'}); 
      return;
    }
}
catch(ex)
{
  console.log("RESET PASSWORD EXCEPTION:"+ex.message);

  logger.error("RESET PASSWORD EXCEPTION:"+ex.message);
}
});

router.post('/auth/verify',function(req,res,next){
    try
    {
    let token=req.body.token;
    let decoded_token=jwt.verify(token,config.secret);
    console.log(decoded_token);
    user.findOne({username:decoded_token.username}).exec((err,userr)=>{
   if(err)
   {
    throw err; 
   }
   if(userr)
   {
    res.status(200).send({message:userr});
   }
   else
   {
    res.send({message:'Invalid'});
   }
  });
}
catch(error)
{
    console.log('There is '+error+' during the process.');
}
});

router.get('/join',user_only_checking,function(req,res,next)
{
    return res.status(200).send({user:''});
});

router.get('/hall',user_only_checking,function(req,res,next)
{
  return res.status(200).send({user:''});
});

router.get('/level',user_only_checking,function(req,res,next)
{
 return res.status(200).send();
});

router.get('/user_profile/:username',user_only_checking,function(req,res,next)
{
     try
     {
      var user_name=req.params.username;
      if(user_name.trim()!="" && user_name!=null)
      {
        user.findOne({username:user_name}).exec((err,user)=>{
          if(err)
          {
            console.log("error:"+err);
            throw err;
          }
         if(!user)
         {
            res.status(404).send({message:'Không tìm thấy user này'});
         }
        else
        {
            res.status(200).send({message:'OK'});
        }
        });
      }
     }
     catch(error)
     {
        throw error;
     }     
});


 router.get('/profile/:username',user_only_checking,function(req,res,next){
     try
     {
      var username=req.params.username;
      res.status(200).send({status:true,message:`Get profile ${username} successfully.`});
     }
     catch(error)
     {
      res.status(401).send({status:false,message:error.message});
     }
 });

 router.get('/device/:username',user_only_checking,function(req,res,next)
 {
  try
  {
   var username=req.params.username;
   res.status(200).send({status:true,message:`Get Device for ${username} successfully.`});
  }
  catch(error)
  {
   res.status(401).send({status:false,message:error.message});
  }
 })

const delay=ms=>new Promise(rs=>setTimeout(rs,ms));

const get_statistic_by_level=async(username:string,level:string)=>
{
try
{
 var username=username; 
    var user_exist=await user.findOne({username:username});
    var user_id=user_exist._id;
    var list_res=[];
    var room_handle=await room_user.aggregate([
      {
          $group:{
            _id:"$_id",
            "level_push":{$push:"$level_id"}
          }
      },
      {
         $match:
         {
             "level_push":
             {
              //$all:[getIdGameLevel(level)]
             }
         }
      }
    ]).exec((err,db)=>{
      if(err)
      {
        throw(err);
      }
      var num=db.length;
      if(num>0)
      { 
        var rooms=[];
        for(let i=0;i<num;i++)
        {
         rooms.push(db[i]._id);
        }
        var count_win=user_room_detail.countDocuments({
          status:"Win",
          user_id:user_id,
          room_id:{$in:rooms},
        },(err,count)=>{
          var num_lose=rooms.length-count;
          list_res.push(count,num_lose);
          return list_res;
        });
      }
      else
      {
       list_res.push(0,0);
       return list_res;
      }
    });
    while(list_res.length==0)
    {
      await delay(100);
    }
    return list_res;
}
catch(err)
{
  console.log(err);
}
}

router.get('/statistics/:username',user_only_checking,async function(req,res,next)
{
  try
  {
   var username=req.params.username;
   console.log(username);
   var list_easy=await get_statistic_by_level(username,'Easy');
   var list_medium=await get_statistic_by_level(username,'Medium');
   var list_hard=await get_statistic_by_level(username,'Hard');
   var list_extreme=await get_statistic_by_level(username,'Extreme');
   console.log('Easy'+" "+list_easy);
   console.log('Medium'+" "+list_medium);
   console.log('Hard'+" "+list_hard);
   console.log('Extreme'+" "+list_extreme);

   res.status(200).send({easy_win:list_easy[0],easy_lose:list_easy[1],medium_win:list_medium[0],medium_lose:list_medium[1],hard_win:list_hard[0],hard_lose:list_hard[1],extreme_win:list_extreme[0],extreme_lose:list_extreme[1]});
  }
  catch(err)
  {
    console.log("Statistic error:"+err);
  }
});

router.post('/auth',user_only_checking,function(req,res,next){
   res.status(200).send({message:'OK'});
});

router.put('/user_detail/:username',user_only_checking,async function(req,res,next)
{
 try{
    var data=req.body;
    console.log("motto:"+data.motto);
    console.log("display_name:"+data.display_name);
    if(username.trim()!="" && username!=null)
    {  
    await user.updateOne(
        {username:data.username},
        {$set:{display_name:data.display_name,
            motto:data.motto,
            avatar:data.avatar,
            gender:data.gender}}
    );
    user.findOne({username:data.username}).exec((err,user)=>
    {  if(user)
        {
     res.status(200).send({message:user});  
        }
    });
}
 }
 catch(err)
 {  res.status(404).send({message:"Cập nhật dữ liệu thất bại:"+err.toString()});
    throw err;
 }
});

router.post('/user_detail/:username',user_only_checking,function(req,res,next)
{
  try
  { 
    var token=uuid.v4();    
    var current_time=Date.now();
    var expire=(current_time/1000)+2400;
    var signature=crypto.createHmac('sha1','private_/h5OYTyHT+iEuJ9X4d4SXbe6w4E=').update(token+expire).digest('hex');
    res.set({
        "Access-Control-Allow-Origin" : "*"
    });
    res.status(200).send({token:token,expire:expire,signature:signature});
  }
  catch(err)
  { 
    throw err;
  }
});
router.get('/user_detail/:username',user_only_checking,function(req,res,next)
{
 try
 {  
    var user_name=req.params.username;

    if(user_name.trim()!="" && user_name!=null)
      {
        user.findOne({username:user_name}).exec((err,user)=>{
          if(err)
          {
            console.log("error:"+err);
            
            throw err;
          }
         if(!user)
        {   logger.error('Get User detail '+user_name+' failed:Cannot find user.');
            
            res.status(404).send({message:'Không tìm thấy user này'});
        }
        else
        {   logger.info('Get User detail '+user_name+' successful');
          
            res.status(200).send({message:'OK'});
        }
        });
      }
 }
 catch(err)
 {
  logger.error("user detail error:"+err.message);
 }
});


router.get('/about',user_only_checking,function(req,res,next)
{
  try
  { logger.info('Access about page successful');
    //var session_time=req.headers['x-session-id'];
    res.status(200).send({status:true,message:'Request success'});
  }
  catch(err)
  {
    console.log('Error loading About page:'+err);
    
    logger.error('Error loading About page:'+err);
  }
});


var downloadImageFromUrl=async(url:string,outputDir:string)=>
{
  try
  { 
  var response = await axios.get(url,{responseType: 'text'});
  const html = response.data;  
  var $=cheerio.load(html);
  console.log($.html());
  var imageUrl:string[]=[];
  $('img').each(async(index,ele)=>
  {
      const src=$(ele).attr('src');

      if(src)
      {
        imageUrl.push(src);
      }
  })
  }
  catch(error)
  {
    console.log("download image error:"+error.message);
  }
  return imageUrl;
}

router.get('/get-rubik',user_only_checking,async function(req,res,next)
{
 try
 {    logger.info('get Rubik successful');
     await rubik_info.find({}).exec((err,element)=>{
    if(err)
    {
      throw err;
    }
    res.status(200).send({status:true,list:element,message:'Lay danh sach rubik thanh cong'});
   });
 }
 catch(err)
 { 
  logger.error('Get rubik failed:'+err.message);

  res.status(401).send({status:false,list:[],message:err.message});
   console.log('Get ruibk list error:'+err.message);
 }
});

router.get('/categories', user_only_checking, async function(req, res, next) {
  try {
    const categories = await category.find({}).sort({ _id: 1 }).lean();
    logger.info('Client fetched categories list successfully');
     console.log("get data successfully:"+categories.length);

    return res.status(200).send({
      tatus: true,
      data: categories,
      message: 'Lay danh sach category thanh cong'
    });
  } catch (err) {
    logger.error('Get categories failed: ' + err.message);
    return res.status(500).send({
      status: false,
      data: [],
      message: err.message
    });
  }
});

router.get('/product-details/:id',user_only_checking,async function(req,res,next){
 try
 { 
  var rubik_id=req.params.id;

  console.log("did here");
  console.log("rubik_id here is:"+rubik_id);
  
  const escapedName = rubik_id.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  // Make apostrophes optional - handles both "Rubiks" and "Rubik's"
  const normalizedName = escapedName.replace(/(Rubik)s/gi, "$1['']?s");
  const regex = new RegExp(`^${normalizedName}$`, "i");

console.log(regex);

  await rubik_info.findOne({name:regex}).exec((err,ele)=>
  {
   if(err)
   {
    throw err;
   }
   console.log("data here is:"+ele);
   logger.info('get product-detail '+rubik_id+' successfull');
   res.status(200).send({status:true,message:'Lay du lieu rubik thanh cong.',data:ele});
  });
 }
 catch(error)
 { 
  var rubik_id=req.params.id;
  logger.error('Get product detail '+rubik_id+' failed:'+error.message);
  res.status(401).send({status:false,message:error.message});
  console.log("Get rubik by id error:"+error.message);
 }
});





router.get('/rubik-solve/:name',user_only_checking,async function(req,res,next)
{
  try
  {
  var rubik_name=req.params.name;
  res.status(200).send({status:true,message:`Token is valid for ${rubik_name} page.`});
  }
  catch(err)
  {
    res.status(401).send({status:false,message:err.message});
  }
  
});


router.post('/product',token_checking,async function(req,res,next)
{
  try{ 
  var rubik_name=req.body.productname;
  var rubik_description=req.body.description;
  var avatar_url=req.body.url;
  var rubik_feature=req.body.feature;

  var check_exist=await rubik_info.find({name:rubik_name}).exec((err,data)=>{

    if(err)
    {
      throw err;
    }
    res.status(401).send({status:false,message:'This product name already existed in the system.'});
  });

  var rubik_ob=
  {
     name:rubik_name,
     description:rubik_description,
     avatar:avatar_url,
     feature:rubik_feature
  };
  var product=new rubik_info(rubik_ob);
  product.save((err,data)=>
  {
     if(err)
     {
      throw err;
     }
     res.status(200).send({status:true,message:'Add product successfully',data:data});
  });
} 
catch(err)
{ 
  res.status(401).send({status:false,message:err.message});
  console.log('There is error while creating the product:'+err.message);
}
});

router.get('/product',token_checking,function(req,res,next){
    try
    {
   res.status(200).send({status:true,message:'Load Add-Product page success'});
    }
    catch(error)
    {
      console.log('Error loading add-product page.');
    }
});



var check_status_device=async(username:string)=>
  {
    var user_info=await user.findOne({username:username});
    if(!user_info.is_checking)
  {
    await user.updateOne({username:username},{$set:{is_checking:true}});
   var check_status= setInterval(async()=>{

    var list_device=await device.find({username:username});
        for(let devic of list_device)
          {
           var now_str =DateTime.now().toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
           var device_time_str=devic.online_time;
          var now=DateTime.fromFormat(now_str,"MMMM dd, yyyy 'at' h:mm:ss a 'GMT'Z");
          var device_time= DateTime.fromFormat(device_time_str,"MMMM dd, yyyy 'at' h:mm:ss a 'GMT'Z");
          const diff_to_seconds = now.diff(device_time,'seconds').seconds;
          if(diff_to_seconds>60)
            {
             await device.updateOne({device_name:devic.device_name},{$set:{status:false}});
            } 
          }
          var now_str =DateTime.now().toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
          var now=DateTime.fromFormat(now_str,"MMMM dd, yyyy 'at' h:mm:ss a 'GMT'Z");
          var last_active_str=user_info.last_active;
          var last_active=DateTime.fromFormat(last_active_str,"MMMM dd, yyyy 'at' h:mm:ss a 'GMT'Z");
          var diff_to_minutes=now.diff(last_active,'minutes').minutes;
          if(diff_to_minutes>=30)
            {
             await user.updateOne({username:username},{$set:{is_checking:false}});
             clearInterval(check_status);
            }
    },30000);
    }
    else{
      console.log("is checking is true");
    }
  }
  
router.get('/mqtt_check_device_status/:username',user_only_checking,async function(req,res,next)
{
 try
 {
  var username=req.params.username;
  await check_status_device(username);
 }
 catch(err)
 {
  console.log("CHECK_STATUS_ERROR:"+err.message);
  logger.error("CHECK_STATUS_ERROR:"+err.message);
  res.status(401).send({status:false,message:err.message});
 }
});

router.post('/reset_checking_status',user_only_checking,async function(req,res,next)
{
  try
  {
   var username = req.body.username;
   await user.updateOne({username:username,$set:{is_checking:false}});
   res.status(200).send({status:true,message:'Reset Checking Status Successfully'});
  }
  catch(ex)
  {
    console.log("Reset Checking Status Exception:"+ex.message);
    logger.error("Reset Checking Status Exception:"+ex.message);
    res.status(400).send({status:false,message:ex.message});
  }
});


router.get('/mqtt_consumer_run',user_only_checking,async function(req,res,next){
 try
 {
  await consumer.run({
    eachMessage:async({topic,partition,message})=>{        
    }
  })
 }
 catch(err)
 {
  console.log("MQTT CONSUMER RUN:"+err.message);
  logger.err("MQTT CONSUMER RUN:"+err.message);
 }
});


const consumer_run=async()=>
{
try
{
  await consumer.run({
    eachMessage:async({topic,partition,message})=>{
      
      if(subscribe_list.includes(topic))
        {
        console.log("this topic exist here");
        var message_topic=message.value.toString();
        var modifield_topic=topic.replace(`${username}_`,'');
        if(message_topic=='CONNECT')
        {
        var updated_date=DateTime.now().toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
        await device.updateOne({device_name:modifield_topic},{$set:{status:true,online_time:updated_date}});
        }
        else if(message_topic=="DISCONNECT")
          {
            var updated_date=DateTime.now().toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
            await device.updateOne({device_name:modifield_topic},{$set:{status:false,online_time:updated_date}});
          }
        else
        {
          received_message=message.value.toString();
          await delay(200);
        }
        }
        console.log('The info received is:',{
          topic,
          partition,
          value:message.value.toString()
        });
    } 
   });
}
catch(ex)
{

}
}


router.get('/products',user_only_checking,async function(req,res,next)
{
 try
 {
    res.status(200).send({status:true,message:'Get Product Page Successfully'});  
 }
 catch(ex)
 { 
  res.status(400).send({status:false,message:ex.message});
  logger.error('GET PRODUCTS EXCEPTION:'+ex.message);
 }
});

router.get('/mqtt_connect/:username',async function(req,res,next){
  try
  {
  var username=req.params.username;
  
  console.log("MQTT CONNECT:"+username);
  res.setHeader('Content-Type','text/event-stream');
        res.setHeader('Cache-Control','no-cache');
        res.setHeader('Connection', 'keep-alive');
        res.setHeader('X-Accel-Buffering','no');
        res.flushHeaders();
  
  var send_log_interval=setInterval(()=>{
     if(received_message!='' && received_message !=null)
      {
      res.write(`event:message\n`);
      res.write(`data:${received_message}\n\n`);
      logger.info("Send Solve Command Successfully:"+received_message);
      received_message='';
      }
  },100);
  req.on('close',()=>{
    logger.info("Close Log SSE Successfully.");
    clearInterval(send_log_interval);
    
  });
  // var list_device=await device.find({username:username});
  
  //  for(const device of list_device)
  //   {
  //     var topic=device.device_name;
  //     var topic_name=`${username}_${topic}`;
  //     const ob_topic=
  //     {topic:topic_name,
  //     numPartitions: 1,
  //     replicationFactor: 1
  //     };
  //     console.log(JSON.stringify(ob_topic)+"\n");
  //     list_topic.push(ob_topic);
  //     subscribe_list.push(topic_name);
  //   }
  //   await admin.createTopics({
  //     waitForLeaders:true,
  //     topics:list_topic
  //   });
  //  console.log('Topic created successfully');
  //  await consumer.subscribe({topics:subscribe_list,fromBeginning:true});
   
  //  req.on('close',()=>{
  //    res.end();
  //  });
   //res.status(200).send({status:true,message:'Connect Mqtt Success.'});
  }
  catch(err)
  {
    console.log('MQTT CONNECT FAILED:'+err.message);
    logger.error("MQTT CONNECT FAILED:"+err);
    res.status(401).send({status:false,message:err.message});
  }
});

router.post('/mqtt_transmit',async function(req,res,next)
{
 try
 { 
  var topic=req.body.topic;
  var content=req.body.command;
  // await consumer.subscribe({topics:[topic],fromBeginning:true});
  await producer.send({
    topic:topic,
    messages:[{
     value:content,
    },]
  });
  logger.info("Send Content To Topic Successfully.");
 }
 catch(err)
 {
  console.log("Mqtt Transmit failed:"+err.message);
  logger.error("MQTT TRANSMIT FAILED:"+err.message);
  res.status(401).send({status:false,message:err.message});
 }
});

router.get('/mqtt_transmit', function(req,res,next){
 try{
  res.setHeader('Content-Type','text/event-stream');
  res.setHeader('Cache-Control','no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering','no');
  res.flushHeaders();
  const interval=setInterval(()=>{
    res.write("event:message\n");
    res.write(`data: ${JSON.stringify({ message: 'Hello from server!' })}`);
    res.write('\n\n');
    // res.end();
  },5000);
  req.on('close',()=>
  {
   clearInterval(interval);
   res.end();
  })
 }
 catch(errr)
 {
  logger.error("GET MQTT_TRANSMIT ERROR:"+errr.message);
 }
});

router.post('/solve_rubik/:name',user_only_checking,async function(req,res,next)
{
try{
  var rubik_name=req.params.name;
  console.log(rubik_name);
  var colors=req.body.colors;
  var device_name=req.body.device_name;
  var username=req.body.username;
  var topic_name=`${username}_${device_name}`;
  console.log('Color is:'+colors);
  var face_convert=convertRubikAnno(colors);
  console.log('rubik after converting:'+face_convert);
  console.log(face_convert.length);
  if(rubik_name =="Rubik's 3x3")
  {  
    var payload={name:rubik_name,facelets:face_convert,original_cube:'',des_cube:''}
    var response=await axios.post(`${process.env.THIRD_PARTY_IP}/solve_rubik`,payload).then(async(result)=>
    {   
        var sol=result.data.data;
        if(sol!=='')
          { 
            await producer.send({
              topic:topic_name,
              messages:[{
               value:sol,
              },]
            });
          var user_id=0;
          const current_user=await user.findOne({username:username});
          user_id=current_user._id;
            const rubik_problem_ob=
            {
              problem:face_convert,
              solution:sol
            };
           
           
       const rubik_problem=new rubikProblem(rubik_problem_ob);

           rubik_problem.save((err,savedOb)=>{
              if(err)
                {
                  throw err;
                }
              logger.info("Rubik problem saved");
              var problem_id=savedOb._id;
              var date_created = DateTime.now().toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
              const problem_detail_ob=
              {
                user_id:user_id,
                problem_id:problem_id,
                date_created:date_created
              };

              const problem_detail=new rubikProblemDetail(problem_detail_ob);

              problem_detail.save((err,savedOb)=>{
                if(err)
                  {
                    throw err;
                  }
                logger.info("Problem detail saved");
              })
           });
            res.status(200).send({status:true,message:sol});
          }
        else 
        {
          res.status(401).send({status:false,message:'Get solution failed'});
        }        
    }).catch(err=>{
      console.log("rubik data:"+err);
      res.status(401).send({status:false,message:err});
    });
    // console.log('here already');
    // const cube_val= new Cube();
    // console.log('before moving:'+cube_val.asString());
    // var temp='LLDDUDDBFUBRURFBUDBLRUFFUFULRRRDBDLFUFLBLRBDBFUFLBRLDR';
    // cube_val.move("D F U");
    // console.log('after moving:'+face_convert);
    // console.log('after moving:'+cube_val.asString());
    
    // if(face_convert===(cube_val.asString()))
    //   {
    //     console.log('Equals');
    //   }
    //   else
    //   {
    //     console.log('Not equals');
    //   }
  }
}
catch(err)
{ 
  console.log('Solve rubik exception:'+err.message);
}
});

router.get('/download_img',async function(req,res,next)
{
  try
  {
  console.log('This api has been called');
  const url='https://rubiks.com/en-US/products/';
  var imageList=await downloadImageFromUrl(url,'');
  console.log("The size of image list is:"+imageList.length);
  if(imageList!=null)
  {
    imageList.forEach((val,idx)=>
    { 
    });
  }
  else
  {
    console.log('The Image List is null');
  }
  }
  catch(error)
  {
    console.log('get img error:'+error.message);
  }
});

module.exports = router;