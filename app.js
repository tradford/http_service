require('dotenv').config();

const express = require('express');
const LocalStrategy = require('passport-local').Strategy;
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const bodyParser = require('body-parser');


const passport = require('passport'); //

const https = require('https');
const fs = require('fs');
const path = require('path');

const { sendTeamsMessage, logMessage } = require('./utils/msteam');
const { sslPrivateKeyPath, sslCertificatePath, capath, ca1path } = require('./config/certIndex')
const shipmentsRouter = require('./routes/shipments');



const app = express();

app.use(passport.initialize());

//app.use(bodyParser.json({ limit: '500mb' }));
//app.use(bodyParser.urlencoded({ limit: '500mb', extended: true }));
//app.use(express.json());
const webHook = 'webhook' // Add webhook

// const app = express();

// app.use(passport.initialize());

// app.use(bodyParser.json({ limit: '500mb' }));
// app.use(bodyParser.urlencoded({ limit: '500mb', extended: true }));

passport.use(new LocalStrategy(
    async (username, password, done) => {
        try {
            // Query your database to find the user by username
            const user = await User.findOne({ username: username });
            
            if (!user) {
                return done(null, false, { message: 'User not found.' });
            }
            
            // Compare the hashed password
            const isMatch = await bcrypt.compare(password, user.password);
            
            if (isMatch) {
                return done(null, user);
            } else {
                return done(null, false, { message: 'Incorrect password.' });
            }
        } catch (err) {
            return done(err);
        }
    }
));

// Setup middleware and routes
app.use('/', shipmentsRouter);
// ... other app configuration ...
// Login route
const urlencodedParse = express.urlencoded({ extended: true }); // Middleware to parse application/x-www-form-urlencoded

// Login route
app.post('/login', urlencodedParse,  (req, res) => {
    // Extract client_id and client_secret from the request body
    const { client_id, client_secret } = req.body;

    // Replace with your authentication logic
    if (client_id === 'client_id' && client_secret === 'client_pw') {
        // User authenticated successfully, create a JWT
        const token = jwt.sign({ client_id: client_id }, 'your-jwt-secret', {
            expiresIn: '1h' // 1 hour
        });

        // Log successful authentication
        const logFilePath = 'path/to/log/file.txt';
        logMessage(logFilePath, 'User: ' + client_id + ' authenticated successfully');

        // Send a message to MS Teams
        sendTeamsMessage(webHook, 'User: ' + client_id + ' authenticated successfully', 'user')
            .then(() => {
                logMessage(logFilePath, 'Message sent to MS Teams');
            })
            .catch(error => {
                sendTeamsMessage(webHook, 'Error during authentication', 'user');
                logMessage(logFilePath, 'Error sending message to MS Teams: ' + error);
            });

        // Token response
        return res.status(200).json({
            token_type: "Bearer",
            expires_in: 3600, // Expires in 1 hour
            access_token: token
        });

    } else {
        // Invalid client_id or client_secret
        return res.status(401).json({ message: "Invalid client credentials" });
    }
});


// ... other routes ...
app.get('/', (req, res) => {
    res.send('Hello World!');
});
////SSL Certificate and Private Key setup
// SSL Certificate and Private Key
const privateKey = fs.readFileSync(sslPrivateKeyPath, 'utf8');
const certificate = fs.readFileSync(sslCertificatePath, 'utf8');

// Intermediate Certificates
const ca1 = fs.readFileSync(capath, 'utf8');
const ca2 = fs.readFileSync(ca1path, 'utf8');

// Combine intermediate certificates
const ca = [ca1, ca2];

const credentials = { key: privateKey, cert: certificate, ca: ca};
// Creating HTTPS server
https.createServer(credentials, app).listen(443, () => {
    console.log('HTTPS Server running on site');
});


