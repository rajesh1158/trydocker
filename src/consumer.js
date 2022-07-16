const { Kafka, logLevel } = require('kafkajs');

const kafka = new Kafka({
	logLevel: logLevel.INFO,
	brokers: ['localhost:29092', 'localhost:39092'],
	clientId: 'example-consumer',
});

const topic = 'topic-test';
const consumer = kafka.consumer({ groupId: 'test-group' });

const run = async () => {
	await consumer.connect();
	await consumer.subscribe({ topic, fromBeginning: false });
	await consumer.run({
		eachMessage: async ({ topic, partition, message }) => {
			const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
			console.log(`- ${prefix} ${message.key}#${message.value}`)
		},
	});
};

run().catch(e => console.error(`[example/consumer] ${e.message}`, e));

const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.forEach(type => {
	process.on(type, async e => {
		try {
			console.log(`process.on ${type}`);
			console.error(e);
			await consumer.disconnect();
			process.exit(0);
		} catch (_) {
			process.exit(1);
		}
	});
});

signalTraps.forEach(type => {
	process.once(type, async () => {
		try {
			await consumer.disconnect();
		} finally {
			process.kill(process.pid, type);
		}
	});
});
