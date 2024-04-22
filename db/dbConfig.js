

// Database configuration
const config = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    server: process.env.DB_SERVER,
    database: process.env.DB_DATABASE,
    options: {
        trustServerCertificate: true
    },
    connectionTimeout: 60000 // Connection timeout in milliseconds (60 seconds)
};

module.exports = config;