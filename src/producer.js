const { Kafka, CompressionTypes, logLevel } = require('kafkajs');

const kafka = new Kafka({
	logLevel: logLevel.DEBUG,
	brokers: ['localhost:29092', 'localhost:39092'],
	clientId: 'example-producer',
});

const topic = 'topic-test';
const numPartitions = 5;

const doAdminWork = async() => {
	const admin = kafka.admin();
	await admin.connect();

	const topics = await admin.listTopics();
	if(topics.indexOf(topic) == -1) {
		console.log('TOPIC NOT FOUND, SO WILL CREATE IT');
		await admin.createTopics({
			topics: [{
				topic: topic,
				numPartitions: numPartitions,
				replicationFactor: 2
			}],
		});
	} else {
		console.log('TOPIC FOUND, WILL NOT CREATE IT');
	}

	await admin.disconnect();
};

var producer;
const setProducer = () => {
	producer = kafka.producer({
		transactionalId: 'transactional-producer-1',
		maxInFlightRequests: 1,
		idempotent: true,
		transactionTimeout: 10000
	});
};

setProducer();

const genrand = (min, max) => {
	return Math.floor(Math.random() * (max - min + 1) + min);
};

var transaction;

const sendMessage = async() => {
	var val = genrand(100000, 10000000000);

	try {
		transaction = await producer.transaction();
		const res = await transaction.send({
			topic: topic,
			compression: CompressionTypes.GZIP,
			messages: [{
				key: '' + (val % 10),
				value: '' + val
			}],
		});
		console.log(res);

		await transaction.commit();
		console.log('Txn committed...');
	} catch(e) {
		console.error(`[example/producer] ${e.message}`, e);
		if(transaction) {
			await transaction.abort();
		}
	}
};

const run = async () => {
	await doAdminWork();
	await producer.connect();
	setInterval(async() => {
		await sendMessage();
	}, 1000);
};

run().catch(e => console.error(`[example/producer] ${e.message}`, e));

const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.forEach(type => {
	process.on(type, async () => {
		try {
			console.log(`process.on ${type}`);
			await producer.disconnect();
			process.exit(0);
		} catch (_) {
			process.exit(1);
		}
	});
});

signalTraps.forEach(type => {
	process.once(type, async () => {
		try {
			await producer.disconnect();
		} finally {
			process.kill(process.pid, type);
		}
	});
});
