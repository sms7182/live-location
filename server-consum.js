const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http);
const path = require('path');

const { Kafka } = require('kafkajs');

let kafkaURL = "kafka:9094"; 
const kafkaTopic = "pgdemo.public.gps-data";
const PORT = 7000;

const kafka = new Kafka({
    clientId: 'notification-app',
    brokers: [kafkaURL] 
});
const consumer = kafka.consumer({ "groupId": 'notification-app-group' });

let lastKnownLocation = null;

app.use(express.static(path.join(__dirname, 'public')));


io.on('connection', (socket) => {
    console.log(` A user connected: ${socket.id}`);
    
    if (lastKnownLocation) {
        socket.emit('locationUpdate', lastKnownLocation);
    }

    socket.on('disconnect', () => {
        console.log(`User disconnected: ${socket.id}`);
    });
    
    socket.on('error', (err) => {
        console.error('Socket error:', err);
    });
});

async function runConsumer() {
    try {
        await consumer.connect();
        console.log('🔗 Consumer connected to Kafka');
        
        await consumer.subscribe({
            topic: kafkaTopic,
            fromBeginning: false 
        });

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const messageValue = message.value.toString();
                
                try {
                    const debeziumPayload = JSON.parse(messageValue);
                    const newRowData = debeziumPayload.after;
                    
                    if (newRowData) {
                        const { id, deviceId, latitude, longitude } = newRowData;
                        
                        console.log(`CDC Record: Device:${deviceId} Lat:${latitude} Long:${longitude}`);

                        const locationUpdate = {
                            lat: parseFloat(latitude),
                            lng: parseFloat(longitude),
                            id: deviceId,
                            timeStamp: id 
                        };

                        lastKnownLocation = locationUpdate;
                        if(deviceId=='live-moji-24')                        
                           io.emit('locationUpdate', locationUpdate);
                        
                    } else if (debeziumPayload.before && !debeziumPayload.after) {
                        console.log(`Record deleted: ${debeziumPayload.before.deviceId}`);
                    }
                    
                } catch (err) {
                    console.error(`Error parsing Debezium message: ${err.message}`);
                }
            },
        });
    } catch (error) {
        console.error('Fatal Error running Kafka consumer:', error);
        process.exit(1); 
    }
}


http.listen(PORT, async () => {
    console.log(`🌎 Server running on http://localhost:${PORT}`);
    
  await runConsumer();
});

process.on('SIGINT', async () => {
    console.log('\nShutting down consumer...');
    try {
        await consumer.disconnect();
    } catch (e) {
        console.error("Error during consumer disconnect:", e);
    }
    process.exit(0);
});