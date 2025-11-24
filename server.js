
const { timeStamp } = require('console');
const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http);
const path = require('path');
const { Pool } = require('pg');

app.use(express.static(path.join(__dirname, 'public')));

let currentLocation = {
    lat: 51.505,
    lng: -0.09,
    id: 'tracker1' 
};
const pool =new Pool({
    user:'postgres',
    host:'',
    database:'trafficdb',
    password: '',
    port:5432,
})

pool.on('error',(err,client)=>{
    console.error('Unexpected error on idle client',err);
});

app.use(express.static(path.join(__dirname,'public')))

let lastKnowLocation={
    lat:0,
    lng:0,
    id:'live-moji-24',
    timeStamp: new Date(0)
}
async function fetchLatestLocation(){
    try{
        const res= await pool.query(`
                SELECT
                g."deviceId",
                g.id, 
                ST_Y(l.geo_point::geometry) AS latitude,
                ST_X(l.geo_point::geometry) AS longitude
            FROM
                public."gps-data" g
            JOIN
                location_instances l ON l.gps_id = g.id
            WHERE
                g."deviceId" = $1
            ORDER BY
                g.id DESC 
            LIMIT 1
            `,[lastKnowLocation.id])
            if (res.rows.length>0){
                const latest= res.rows[0]
                const newTimestamp = new Date(latest.id);
                if (newTimestamp>lastKnowLocation.timeStamp){
                    lastKnowLocation={
                        lat:latest.latitude,
                        lng:latest.longitude,
                        id:latest.deviceId,
                        timeStamp:latest.id
                    }
                }
                console.log('Fetch and emitting db update:',lastKnowLocation)
                io.emit('locationUpdate',lastKnowLocation)
            }else{
                console.log('No newer location found')
            }
    }catch(err){
        console.error('Error fetching location from db',err.stack)
    }
    
}
setInterval(fetchLatestLocation, 2000);

io.on('connection', (socket) => {
    console.log('A user connected');
    socket.emit('locationUpdate', currentLocation);

    socket.on('disconnect', () => {
        console.log('User disconnected');
    });
});

const PORT = 7000;
http.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});