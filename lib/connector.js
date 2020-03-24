const amqp = require('amqplib');

/**
 *
 * @param {Function} callback - Takes a parameter with channel when connected
 * @param {string} uri - Takes rabbitmq connection uri
 */
const connector = async (whenConnected, uri) => {
	const retry = (ms) => setTimeout(() => {
		connector(whenConnected, uri);
	}, ms * 1000);

	try {
		console.log('[AMQP Connection]:: Trying to establish connection.');
		const conn = await amqp.connect(uri);
		const channel = await conn.createChannel();
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
		whenConnected(channel);
		console.log('[AMQP Connection]:: Connected');
	} catch (err) {
		console.log('[AMQP Connection]:: Connection failed, retrying.');
		retry(5);
	}
};

module.exports = connector;
