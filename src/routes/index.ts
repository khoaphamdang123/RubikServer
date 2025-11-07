import * as express from 'express'
import {user,room_user,user_room_detail,rubik_info,image_detail,social_account,session,role,rubikProblem,rubikProblemDetail,temp_device,device} from '../models/user_model';
import checkingDuplicateUserNameOrEmail from '../config/checking';
import {token_checking,email_token_checking} from '../config/checkingToken';
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

router.get('/device/:username',token_checking,async function(req,res,next)
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




router.post('/add_images',token_checking,upload.array('images',10),async function(req,res,next)
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

router.post('/add_device',token_checking,async function(req,res,next)
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


router.post('/delete_device',token_checking,async function(req,res,next){
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

router.get('/join',token_checking,function(req,res,next)
{
    return res.status(200).send({user:''});
});

router.get('/hall',token_checking,function(req,res,next)
{
  return res.status(200).send({user:''});
});

router.get('/level',token_checking,function(req,res,next)
{
 return res.status(200).send();
});

router.get('/user_profile/:username',token_checking,function(req,res,next)
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


 router.get('/profile/:username',token_checking,function(req,res,next){
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

 router.get('/device/:username',token_checking,function(req,res,next)
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

router.get('/statistics/:username',token_checking,async function(req,res,next)
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

router.post('/auth',token_checking,function(req,res,next){
   res.status(200).send({message:'OK'});
});

router.put('/user_detail/:username',token_checking,async function(req,res,next)
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

router.post('/user_detail/:username',token_checking,function(req,res,next)
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
router.get('/user_detail/:username',token_checking,function(req,res,next)
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


router.get('/about',token_checking,function(req,res,next)
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

router.get('/get-rubik',token_checking,async function(req,res,next)
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

router.get('/product-details/:id',token_checking,async function(req,res,next){
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





router.get('/rubik-solve/:name',token_checking,async function(req,res,next)
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
  
router.get('/mqtt_check_device_status/:username',token_checking,async function(req,res,next)
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

router.post('/reset_checking_status',token_checking,async function(req,res,next)
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


router.get('/mqtt_consumer_run',token_checking,async function(req,res,next){
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


router.get('/products',token_checking,async function(req,res,next)
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

router.post('/solve_rubik/:name',token_checking,async function(req,res,next)
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