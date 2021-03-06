const nanoid = require('nanoid');
const amqp = require('amqplib');

let channel;
const callbacks = {};
const replyTo = nanoid();

const rpcSend = async ({
	exchange, key: routingKey, data, correlationId = nanoid(), next, nextErr = null, res = null,
}) => {
	callbacks[correlationId] = { next, nextErr, res };
	await channel.publish(
		exchange,
		routingKey,
		Buffer.from(JSON.stringify(data)),
		{ correlationId, replyTo },
	);
};

const handleMessage = (msg) => {
	if (msg) {
		if (Object.keys(callbacks).includes(msg.properties.correlationId)) {
			try {
				const content = JSON.parse(msg.content.toString());
				callbacks[msg.properties.correlationId].next(content);
			} catch (err) {
				if (callbacks[msg.properties.correlationId].res) {
					callbacks[msg.properties.correlationId].res
						.status(500)
						.json({ message: 'API Server Error.' });
				}
				if (callbacks[msg.properties.correlationId].nextErr) {
					callbacks[msg.properties.correlationId].nextErr(err);
				}
			} finally {
				delete callbacks[msg.properties.correlationId];
			}
		}
	}
};

const whenConnectedInternally = async (ch) => {
	await ch.assertQueue(replyTo, { exclusive: true, autoDelete: true });
	await ch.consume(replyTo, handleMessage, { noAck: true });
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
		conn.on('error', (err) => {
			console.log('[AMQP Connection]:: Error occurred with connection.', err);
		});
		conn.on('close', (err) => {
			if (err.code === 320) {
				console.log('[AMQP Connection]:: Connection closed, retrying.');
				retry(5);
			} else console.log(err);
		});
		channel.on('error', (err) => {
			console.log('[AMQP Connection]:: Error occurred with channel.', err);
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
