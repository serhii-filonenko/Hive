const Connection = require('thrift').Connection;
const net = require('net');

const createKerberosConnection = (kerberosAuthProcess) => (host, port, options) => {
	return kerberosAuthentication(kerberosAuthProcess)({
		authMech: 'GSSAPI',
		krb_host: options.krb5.krb_host,
		krb_service: options.krb5.krb_service,
		username: options.krb5.username,
		password: options.krb5.password,
		host,
		port,
	}).then(({ connection }) => {
		const conn = new Connection(connection, options);

		conn.host = host;
		conn.port = port;
		connection.emit('connect');

		return conn;
	});
};

const kerberosAuthentication = (kerberosAuthProcess) => (options) => new Promise((resolve, reject) => {
	const inst = new kerberosAuthProcess(
		options.krb_host,
		options.port,
		options.krb_service
	);

	const START = 1;
	const OK = 2;
	const BAD = 3;
	const ERROR = 4;
	const COMPLETE = 5;

	const sendMessage = (status, body) => {
		const bodyLength = new Buffer(4);

		bodyLength.writeUInt32BE(body.length);

		return Buffer.concat([ new Buffer([ status ]), bodyLength, body ]);
	};

	inst.init(options.username, options.password, (err, client) => {
		if (err) {
			return reject(err);
		}

		const stream = net.createConnection(options.port, options.host);
		const onError = (err) => {
			stream.end();

			reject(err);
		};
		const onSuccess = () => {
			stream.removeListener('connect', onConnect);
			stream.removeListener('data', onData);

			resolve({
				krbClient: client,
				connection: stream
			});
		};
		const onConnect = () => {
			stream.write(sendMessage(START, new Buffer(options.authMech)));

			inst.transition('', (err, token) => {
				if (err) {
					return onError(err);
				}

				stream.write(sendMessage(OK, new Buffer(token || '', 'base64')));
			});
		};
		const onData = (data) => {
			const result = data[0];

			if (result === OK) {
				const payload = data.slice(5).toString('base64');
					
				inst.transition(payload, (err, response) => {
					if (err) {
						return onError(err);
					}

					stream.write(sendMessage(OK, new Buffer(response || '', 'base64')));
				});
			} else if (result === COMPLETE) {
				onSuccess();
			} else {
				const message = data.slice(5).toString();

				onError(new Error('Authenticated error: ' + message));
			}
		};

		stream.addListener('connect', onConnect);
		stream.addListener('data', onData);
	});
});

module.exports = createKerberosConnection;
