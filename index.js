const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();

app.use(express.json());

app.use(express.urlencoded({ extended: true }));


//Kafka client setup
const kafka = new Kafka({
    clientId: 'kafka-producer',
    brokers: ['localhost:9092']
}); 
const producer = kafka.producer();


const initKafkaProducer = async () => {
    try {
        await producer.connect();
        console.log('Kafka:Producer connected');
    } catch (error) {
        console.log('Kafka:Connection is failed')
        process.exit(1)       
    }
}

app.post('/send', async (req, res) => {
    try {
        await producer.send({
            topic: 'order',
            messages: [
                { value: 'test1' }
            ]
        });
        res.status(200).json({ message: 'Data sent to Kafka' });
        
    } catch (error) {
        console.log('Kafka:Error while sending data =>', error.message);
        res.status(500).json({ message: 'Error while sending data to Kafka' });
        
    }
})


app.listen(7000, async() =>{
    await initKafkaProducer();
    console.log('KAFKA ayaktayiz');
})