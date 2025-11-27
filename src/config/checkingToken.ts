import {session, user,device,role} from "../models/user_model"
import {DateTime} from 'luxon';
const jwt=require('jsonwebtoken');
const config=require('./auth');
const logger=require('../logger/index');
var token_checking=async(req,res,next)=>{
 var token=req.header('authorization');
 console.log('token here is:'+token);
 if(!token)
 {  return res.status(401).send({message:"No token is provided"});
 }
 var existingToken=await session.findOne({token:token});
 if(!existingToken)
  { logger.error("Your account has been login from another place.");
    return res.status(401).send({message:"Your account has been login from another place"});
  }
 jwt.verify(token,config.secret,async (err,decoded)=>{
  if(err)
  { 
    return res.status(401).send({message:"Unauthorized"});
  }
  req.userId=decoded.id;
  var user_info=await user.findOne({username:decoded.username});
  if(user_info)
    {
  var last_active_action=user_info.last_active;
  var now_str=DateTime.now().toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
  await user.updateOne({username:user_info.username},{$set:{last_active:now_str,last_action:last_active_action}});
    }
  next();
 }) 
}

// 管理员中间件：检查用户是否是管理员
var admin_checking=async(req,res,next)=>{
 try {
   var token=req.header('authorization');

   const adminLoginUrl='http://localhost:3000/admin/login';
   
   if(!token)
   {  
     return res.status(401).json({
       status:false,
       message:"No token is provided",
       redirect:adminLoginUrl
     });
   }
   
   var existingToken=await session.findOne({token:token});
   
   if(!existingToken)
   { 
     logger.error("Session expired or account logged in from another place.");
     console.log("Session expired or account logged in from another place.:"+token);
     return res.status(401).json({
       status:false,
       message:"Session expired or account logged in from another place",
       redirect:adminLoginUrl
     });
   }
   
   jwt.verify(token,config.secret,async (err,decoded)=>{
     if(err)
     { 
       logger.error("JWT verification failed: "+err.message);
       return res.status(401).json({
         status:false,
         message:"Session expired. Please login again",
         redirect:adminLoginUrl
       });
     }
     
     var user_info=await user.findOne({username:decoded.username});
     if(!user_info)
     {
       return res.status(401).json({
         status:false,
         message:"User not found",
         redirect:adminLoginUrl
       });
     }
     
     // 查找用户的角色信息
     var user_role=await role.findOne({_id:user_info.role_id});
     if(!user_role)
     {
       return res.status(403).json({
         status:false,
         message:"Role not found",
         redirect:adminLoginUrl
       });
     }
     
     // 检查是否是管理员角色（role_type为'Admin'）
     if(user_role.role_type!=='Admin')
     {
       logger.error(`Unauthorized admin access attempt by user: ${decoded.username}`);
       return res.status(403).json({
         status:false,
         message:"Access denied. Admin privileges required.",
         redirect:adminLoginUrl
       });
     }
     
     // 更新用户最后活动时间
     var last_active_action=user_info.last_active;
     var now_str=DateTime.now().toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
     await user.updateOne({username:user_info.username},{$set:{last_active:now_str,last_action:last_active_action}});
     
     req.userId=decoded.user_id || decoded.id;
     req.username=decoded.username;
     req.userRole=user_role.role_type;
     next();
   });
 }
 catch(error)
 {
   logger.error("Admin checking error: "+error.message);
   const adminLoginUrl='http://localhost:3000/admin/login';
   return res.status(500).json({
     status:false,
     message:"Internal server error",
     redirect:adminLoginUrl
   });
 }
}


// 用户专用中间件：只允许普通用户访问，管理员访问时删除session并重定向到登录页
var user_only_checking=async(req,res,next)=>{
 try {
   var token=req.header('authorization');
   if(!token)
   {  
     return res.status(401).send({message:"No token is provided"});
   }

   
   var existingToken=await session.findOne({token:token});
   if(!existingToken)
   { 
     logger.error("Your account has been login from another place.");
     return res.status(401).send({message:"Your account has been login from another place"});
   }
   
   jwt.verify(token,config.secret,async (err,decoded)=>{
     if(err)
     { 
       return res.status(401).send({message:"Unauthorized"});
     }
     
     var user_info=await user.findOne({username:decoded.username});
     
     if(!user_info)
     {
       return res.status(401).send({message:"User not found"});              
     }
     
     // 查找用户的角色信息
     var user_role=await role.findOne({_id:user_info.role_id});

     if(!user_role)
     {
       return res.status(403).send({message:"Role not found"});
     }
     
     // 检查是否是管理员角色 - 如果是管理员，删除session并重定向到登录页
     if(user_role.role_type==='Admin')
     {
       logger.error(`Admin account attempted to access client page: ${decoded.username}`);
       
       // 删除session token
       await session.deleteOne({token:token});
       
       // 重定向到客户端登录页
       const {loginUrl} = require('../routes/gmail_account');
       return res.status(403).json({
         status:false,
         message:"Admin accounts cannot access client pages. Session terminated.",
         redirect:loginUrl
       });
     }
     
     // 更新用户最后活动时间
     var last_active_action=user_info.last_active;
     var now_str=DateTime.now().toLocaleString(DateTime.DATETIME_FULL_WITH_SECONDS);
     await user.updateOne({username:user_info.username},{$set:{last_active:now_str,last_action:last_active_action}});
     
     req.userId=decoded.user_id || decoded.id;
     req.username=decoded.username;
     req.userRole=user_role.role_type;
     next();
   });
 }
 catch(error)
 {
   logger.error("User only checking error: "+error.message);
   return res.status(500).send({message:"Internal server error"});
 }
}

var email_token_checking=(req,res,next)=>{
  var email_token=req.query.token;
  
  if(!email_token)
  {
    return res.status(401).send({message:'Không tìm thấy token'});
  }

 try{
   var decoded_token=jwt.verify(email_token,config.secret);
   if(decoded_token)
   {
     var token_expire_time=decoded_token.exp;
     var datetime_now=new Date();
     var datetime_epoch=datetime_now.getTime()/1000;
     if(token_expire_time<datetime_epoch)
     {  
        var expired_link='http://localhost:3000/expired_token';
        res.redirect(301,expired_link);
     }
   }
   next();
}
catch(error)
{                                                     
    var expired_link='http://localhost:3000/expired_token';
    res.redirect(expired_link);        
}
}


export {token_checking,email_token_checking,admin_checking,user_only_checking};
