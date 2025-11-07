import mongoose, { mongo } from "mongoose";
require('dotenv').config();
const Connection=()=>{
    const URL=process.env.DATABASE_URI_BACKUP;
    console.log("Database URL:",URL);
    try{

        mongoose.connect(URL);
        console.log("Database connected successfully");
    }
    catch(error)
    {
     console.log('Error while connecting the database:',error);
    }
}
export default Connection;