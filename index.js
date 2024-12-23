//const express = require('express');
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-kafka-producer2',
  brokers: ['localhost:9092']
})

//const producer = kafka.producer()
const consumer = kafka.consumer({ groupId: 'my-kafka-producer2' })

const run = async () => {
  // Consuming
  await consumer.connect()
  await consumer.subscribe({ topic: 'order', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
        createInvoice(message)
        console.log({
            partition,
            offset: message.offset,
            value: message.value.toString(),
        })
        },
    })
}

function createInvoice(message){
    console.log('Fatura olusturuldu');
    console.log(message);
    return true
}

run().catch(console.error)
/*
app.listen(7000, async() =>{
    await initKafkaProducer();
    console.log('KAFKA ayaktayiz');
})
*/