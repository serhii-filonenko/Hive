const net = require('net');

const createPackage = (body) => {
	const bodyLength = new Buffer(4);

	bodyLength.writeUInt32BE(body.length);

	return Buffer.concat([ bodyLength, body ]);
};

const tcpConnection = (port, host, options) => {
	let stream;
	let encoder = (data, cb) => cb(null, data); 

	return {
		connect() {
			stream = net.createConnection(port, host);

			return stream;
		},

		assignStream(connection, client) {
			const savedWrite = connection.prototype.write;
			const savedReceiver = options.transport.receiver;

			connection.prototype.write = function (data) {
				client.wrap(data.slice(4).toString('base64'), { encode: 1 }, (err, encodedData) => {
					if (err) {
						throw err;
					}

					const payload = Buffer.from(encodedData, 'base64');

					savedWrite.call(this, createPackage(payload));
				});
			};

			options.transport.receiver = (handle) => {
				const mainReceiver = savedReceiver(handle);

				return (data) => {
					client.unwrap(data.slice(4).toString('base64'), (err, decodedData) => {
						if (err) {
							throw err;
						}

						const payload = Buffer.from(decodedData, 'base64');

						mainReceiver(createPackage(payload));
					});
				};
			};

			const conn = new connection(stream, options);

			conn.host = host;
			conn.port = port;
			stream.emit('connect');
	
			return conn;
		},

		addListener(eventName, callback) {
			stream.addListener(eventName, callback);
		},

		removeListener(eventName, listener) {
			stream.removeListener(eventName, listener);
		},

		write(data) {
			stream.write(data);
		},

		end() {
			return stream.end();
		}
	};
};

module.exports = tcpConnection;
