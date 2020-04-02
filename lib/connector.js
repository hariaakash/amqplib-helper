const nanoid = require('nanoid');
const amqp = require('amqplib');

let channel;
const callbacks = {};
const replyTo = nanoid();

const rpcSend = async ({
	exchange, key: routingKey, data, correlationId = nanoid(), next,
}) => {
	callbacks[correlationId] = next;
	await channel.publish(
		exchange,
		routingKey,
		Buffer.from(JSON.stringify(data)),
		{ correlationId, replyTo },
	);
};

const whenConnectedInternally = async (ch) => {
	await ch.assertQueue(replyTo, { exclusive: true, autoDelete: true });
	await ch.consume(replyTo, (msg) => {
		if (msg) {
			if (Object.keys(callbacks).includes(msg.properties.correlationId)) {
				const content = JSON.parse(msg.content.toString());
				callbacks[msg.properties.correlationId](content);
			}
		}
	}, { noAck: true });
};

/**
 *
 * @param {Function} callback - Takes a parameter with channel when connected
 * @param {string} uri - Takes rabbitmq connection uri
 */
const connector = async (uri, whenConnectedExternally) => {
	const retry = (ms) => setTimeout(() => {
		connector(uri, whenConnectedExternally);
	}, ms * 1000);

	try {
		console.log('[AMQP Connection]:: Trying to establish connection.');
		const conn = await amqp.connect(uri);
		channel = await conn.createChannel();
		conn.on('error', () => {
			console.log('[AMQP Connection]:: Error occurred with connection.');
		});
		conn.on('close', (err) => {
			if (err.code === 320) {
				console.log('[AMQP Connection]:: Connection closed, retrying.');
				retry(5);
			}
		});
		channel.on('error', () => {
			console.log('[AMQP Connection]:: Error occurred with channel.');
		});
		channel.on('close', () => {
			console.log('[AMQP Connection]:: Channel closed.');
		});
		await whenConnectedInternally(channel);
		await whenConnectedExternally(channel);
		console.log('[AMQP Connection]:: Connected');
	} catch (err) {
		console.log('[AMQP Connection]:: Connection failed, retrying.');
		retry(5);
	}
};

module.exports = { connector, rpcSend };
