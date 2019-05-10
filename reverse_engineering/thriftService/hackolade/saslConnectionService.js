const ThriftConnection = require('thrift').Connection;
const sslTcpConnection = require('./connections/sslTcpConnection');
const tcpConnection = require('./connections/tcpConnection');

const getConnection = (port, host, options) => {
	if (options.ssl) {
		return sslTcpConnection(port, host, options);
	} else {
		return tcpConnection(port, host, options);
	}
};

const createKerberosConnection = (kerberosAuthProcess) => (host, port, options) => {
	const connection = getConnection(port, host, options);

	return kerberosAuthentication(kerberosAuthProcess, connection)({
		authMech: 'GSSAPI',
		krb_host: options.krb5.krb_host,
		krb_service: options.krb5.krb_service,
		username: options.krb5.username,
		password: options.krb5.password,
		host,
		port,
	}).then(({ connection }) => {
		return connection.assignStream(ThriftConnection);
	});
};

const createLdapConnection = (kerberosAuthProcess) => (host, port, options) => {
	const connection = getConnection(port, host, options);

	return ldapAuthentication(kerberosAuthProcess, connection)({
		authMech: 'PLAIN',
		username: options.username,
		password: options.password,
		host,
		port,
	}).then(() => {
		return connection.assignStream(ThriftConnection);
	});
};

const START = 1;
const OK = 2;
const BAD = 3;
const ERROR = 4;
const COMPLETE = 5;

const createPackage = (status, body) => {
	const bodyLength = new Buffer(4);

	bodyLength.writeUInt32BE(body.length);

	return Buffer.concat([ new Buffer([ status ]), bodyLength, body ]);
};

const kerberosAuthentication = (kerberosAuthProcess, connection) => (options) => new Promise((resolve, reject) => {
	const inst = new kerberosAuthProcess(
		options.krb_host,
		options.port,
		options.krb_service
	);

	inst.init(options.username, options.password, (err, client) => {
		if (err) {
			return reject(err);
		}

		connection.connect();
		const onError = (err) => {
			connection.end();

			reject(err);
		};
		const onSuccess = () => {
			connection.removeListener('connect', onConnect);
			connection.removeListener('data', onData);

			resolve({
				client: client
			});
		};
		const onConnect = () => {
			connection.write(createPackage(START, new Buffer(options.authMech)));

			inst.transition('', (err, token) => {
				if (err) {
					return onError(err);
				}

				connection.write(createPackage(OK, new Buffer(token || '', 'base64')));
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

					connection.write(createPackage(OK, new Buffer(response || '', 'base64')));
				});
			} else if (result === COMPLETE) {
				onSuccess();
			} else {
				const message = data.slice(5).toString();

				onError(new Error('Authenticated error: ' + message));
			}
		};

		connection.addListener('connect', onConnect);
		connection.addListener('data', onData);
	});
});

const ldapAuthentication = (kerberosAuthProcess, connection) => (options) => new Promise((resolve, reject) => {
	const inst = new kerberosAuthProcess(
		'',
		'',
		options.port
	);

	inst.init(options.username, options.password, (err, client) => {
		if (err) {
			return reject(err);
		}

		connection.connect();

		const onError = (err) => {
			connection.end();

			reject(err);
		};
		const onSuccess = () => {
			connection.removeListener('connect', onConnect);
			connection.removeListener('data', onData);

			resolve({
				client: client
			});
		};
		const onConnect = () => {
			connection.write(createPackage(START, new Buffer(options.authMech)));
			connection.write(createPackage(OK, Buffer.concat([
				new Buffer(options.username || ""),
				Buffer.from([0]),
				new Buffer(options.username || ""),
				Buffer.from([0]),
				new Buffer(options.password || ""),
			])));
		};
		const onData = (data) => {
			const result = data[0];

			if (result === COMPLETE) {
				onSuccess();
			} else {
				const message = data.slice(5).toString();

				onError(new Error('Authenticated error: ' + message));
			}
		};

		connection.addListener('connect', onConnect);
		connection.addListener('data', onData);
	});
});

exports.createKerberosConnection = createKerberosConnection;
exports.createLdapConnection = createLdapConnection;