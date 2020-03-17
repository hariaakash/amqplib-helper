const nanoid = require('nanoid');

/**
 * RPC Send Wrapper for amqplib supports Promise and Callback
 * @function rpcSend
 * @param {Object} obj- Information required for handling rpc client side.
 * @param {Object} obj.ch - AMQP channel connection
 * @param {String} obj.exchange - Exchange to which data has to be sent.
 * @param {String} obj.key - Routing Key to identify the right listener.
 * @param {Object} obj.data - The data to be sent.
 * @param {Function} obj.next - Callback to handle the response.
 */

const rpcSend = ({
	ch, exchange, key: routingKey, data, next,
}) => new Promise((resolve, reject) => {
	try {
		const correlationId = nanoid();
		const replyTo = nanoid();
		ch.assertQueue(replyTo, { exclusive: true });
		ch.consume(replyTo, (msg) => {
			if (msg) {
				if (msg.properties.correlationId === correlationId) {
					setTimeout(() => ch.deleteQueue(replyTo), 100);
					const content = JSON.parse(msg.content.toString());
					resolve(next ? next(content) : content);
				}
			}
		});
		ch.publish(
			exchange,
			routingKey,
			Buffer.from(JSON.stringify(data)),
			{ correlationId, replyTo },
		);
	} catch (err) {
		reject(err);
	}
});


module.exports = { rpcSend };
