import { createDecipheriv } from "crypto";
import mongoose, { mongo } from "mongoose";
import { SettingsUpdateInstance } from "twilio/lib/rest/supersim/v1/settingsUpdate";
import { Deserializer } from "v8";
var autoIncrement=require('mongoose-auto-increment');
const {Schema}=mongoose;
const userSchema=new Schema({
    _id:{type:Number,required:false,unique:true},
    username:{type:String,required:true},
    password:{type:String,require:true},
    gender:{type:String,required:true},
    email:{type:String,required:true},
    phone:{type:String,required:true},
    avatar:{type:String,required:true},
    created_date:{type:String,required:true},
    last_active:{type:String,required:true},
    last_action:{type:String,required:true},
    is_checking:{type:Boolean,required:true},
    role_id:{type:Number,required:true},
});
autoIncrement.initialize(mongoose.connection);
userSchema.plugin(autoIncrement.plugin,'user');
const user=mongoose.model('user',userSchema,'User');

const sessionSchema = new Schema({
    _id:{type:Number,required:false},
    user_name:{type:String,required:true},
    token:{type:String,required:true},
    created_time:{type:String,required:true},
    ip_address:{type:String,required:true},
    city:{type:String,required:true}
});

sessionSchema.plugin(autoIncrement.plugin,'session');


const session=mongoose.model('session',sessionSchema,'Session');


const rubikProblemSchema = new Schema({
   _id:{type:Number,required:false},
   problem:{type:String,required:true},
   solution:{type:String,required:true}
})

rubikProblemSchema.plugin(autoIncrement.plugin,'problem');

const rubikProblem=mongoose.model('problem',rubikProblemSchema,'rubikProblem');

const rubikProlemDetailSchema=new Schema({
    _id:{type:Number,required:false},
    user_id:{type:Number,required:true},
    problem_id:{type:Number,required:true},
    date_created:{type:String,required:true}
});

rubikProlemDetailSchema.plugin(autoIncrement.plugin,'problem_detail');

const rubikProblemDetail=mongoose.model('problem_detail',rubikProlemDetailSchema,'rubikProblemDetail');

const questionLevelSchema=new Schema({
    _id:{type:Number,required:false},
    level:{type:String,required:true}
});


questionLevelSchema.plugin(autoIncrement.plugin,'question')

const roomSchema=new Schema({
    _id:{type:String,required:false},
    room_name:{type:String,required:true},
    created_date:{type:String,required:true},
    level_id:{type:Number,required:true},
    score:{type:String,required:true}
})

const userRoomDetailSchema=new Schema({
    _id:{type:Number,required:false}, 
    room_id:{type:Number,required:true},
    user_id:{type:Number,required:true},
    status:{type:String,required:true}
});


const rubikInfoSchema = new Schema({
    _id:{type:String,required:false},
    name:{type:String,required:true},
    description:{type:String,required:true},
    avatar:{type:String,required:true},
    feature:{type:String,required:true},
    category_id:{type:Number,required:false}
})

const categorySchema = new Schema({
_id:{type:Number,required:false},
category_name:{type:String,required:true},
created_date:{type:String,required:true},
updated_date:{type:String,required:true}
});

categorySchema.plugin(autoIncrement.plugin,'category');

const category=mongoose.model('category',categorySchema,'Category');

const socialNetworkAccountSchema=new Schema({
    _id:{type:Number,required:false},
    username:{type:String,required:true},
    display_name:{type:String,required:true},
    social_type:{type:String,required:true}
});

const deviceSchema = new Schema(
{
 _id:{type:Number,required:false},
 username:{type:String,required:true},
 device_name:{type:String,required:true},
 created_date:{type:String,required:true},
 status:{type:Boolean,required:true},
 online_time:{type:String,required:true},
})

const tempDeviceSchema=new Schema({
    _id:{type:Number,required:false},
    username:{type:String,required:true},
    device_name:{type:String,required:true},
    created_date:{type:String,required:true}    
})

const detailImageSchema = new Schema({
 _id:{type:Number,require:false},
 rubik_id:{type:Number,require:true},
 _url:{type:String,require:true},
});

const singleBoardDetailSchema=new Schema({
_id:{type:Number,required:false},
user_id:{type:Number,required:true},
level_id:{type:Number,require:true},
time_complete:{type:String,require:true}
});

const roleSchema=new Schema({
    _id:{type:Number,required:false},
    role_type:{type:String,required:true}
});

const question_level=mongoose.model('question',questionLevelSchema,'QuestionLevel');

roomSchema.plugin(autoIncrement.plugin,'room');

userRoomDetailSchema.plugin(autoIncrement.plugin,'userRoomDetail');

singleBoardDetailSchema.plugin(autoIncrement.plugin,'singleBoardDetail');

deviceSchema.plugin(autoIncrement.plugin,'device');

detailImageSchema.plugin(autoIncrement.plugin,'detailImage');

roleSchema.plugin(autoIncrement.plugin,'role');

tempDeviceSchema.plugin(autoIncrement.plugin,'tempDevice');

socialNetworkAccountSchema.plugin(autoIncrement.plugin,'socialNetworkAccount');

const room_user=mongoose.model('room',roomSchema,'Room');

const user_room_detail=mongoose.model('userRoomDetail',userRoomDetailSchema,'UserRoomDetail');

const single_board_detail=mongoose.model('singleBoardDetail',singleBoardDetailSchema,'SingleBoardDetail');

const rubik_info=mongoose.model('rubikInfo',rubikInfoSchema,'RubikInfo');

const device=mongoose.model('device',deviceSchema,'Device');

const temp_device=mongoose.model('tempDevice',tempDeviceSchema,'TempDevice');

const social_account=mongoose.model('socialNetworkAccount',socialNetworkAccountSchema,'SocialNetworkAccount');

const image_detail=mongoose.model('detailImage',detailImageSchema,'DetailImage');

const role=mongoose.model('role',roleSchema,'Role');


export {user,question_level,room_user,user_room_detail,single_board_detail,rubik_info,image_detail,session,temp_device,device,rubikProblem,social_account,rubikProblemDetail,category,role}; 



