const express = require('express');
const { addPayloadToProcess} = require('../services/payloadProcessor');
const { authenticateToken } = require('../utils/auth'); // Import authentication middleware
const fs = require('fs');
const path = require('path');

const router = express.Router();
const { sendTeamsMessage, logMessage } = require('../utils/msteam');

const webHook = 'webhook' // Add webhook

const zlib = require('zlib');

function gzipMiddleware(req, res, next) {
    if (req.headers['content-encoding'] === 'gzip') {
        let chunks = [];
        req.on('data', chunk => chunks.push(chunk));
        req.on('end', () => {
            const buffer = Buffer.concat(chunks);
            zlib.gunzip(buffer, (err, decompressed) => {
                if (err) {
                    console.error('Decompression error:', err);
                    return res.status(200).send('Error while decompressing');
                }
                req.body = decompressed;
                next();
            });
        });
    } else {
        next();
    }
}

function conditionalParser(req, res, next) {
    if (req.headers['content-type'] === 'application/json') {
        if (req.body instanceof Buffer) {
            // Parse the decompressed buffer as JSON
            try {
                req.body = JSON.parse(req.body.toString());
                next();
            } catch (jsonErr) {
                console.error('JSON parsing error:', jsonErr);
                res.status(200).send('Invalid JSON data');
            }
        } else {
            // Use Express's JSON parser for already parsed JSON
            express.json()(req, res, next);
        }
    } else {
        next();
    }
}

const app = express();

const router = require('express').Router();
const fs = require('fs');
const path = require('path');
const { authenticateToken, gzipMiddleware, conditionalParser } = require('./middleware');
const { logMessage, sendTeamsMessage } = require('../utils/msteam');

router.post('/update/v2/shipments', authenticateToken, gzipMiddleware, conditionalParser, async (req, res) => {
    console.log("Request received");
    try {
        if (req.body && req.body.data) {
            // Extract username from the token or the request
            const client_id = req.body.data.id; // Adjust according to how user information is stored in your request

            // Get the current date to use in the file name
            const currentDate = new Date().toISOString();
            const formattedDate = currentDate.replace(/:/g, '-').substring(0, 19);
            const fileName = `${formattedDate}_${client_id}_payload.json`;
       
            // Define the file path in a generic way
            const filePath = path.join('path', 'to', 'backup', fileName);

            fs.writeFile(filePath, JSON.stringify(req.body.data, null, 2), async (err) => {
                if (err) {
                    console.error('Error writing file:', err);
                    res.status(500).send('An error occurred while writing the file');
                    return;
                }

                try {
                    await addPayloadToProcess(req.body.data);
                    res.status(200).send('Payload added to queue and file saved');
                    sendTeamsMessage(webHook, `Payload ${client_id} added to queue and file saved`, 'Notification Group');
                } catch (processError) {
                    console.error('Error processing payload:', processError);
                    res.status(500).send('An error occurred while processing the payload');
                    sendTeamsMessage(webHook, `Payload processed with error: ${processError}`, 'Notification Group');
                }
            });
        } else {
            sendTeamsMessage(webHook, 'Payload does not have the proper structure to consume', 'Notification Group');
            res.status(400).send('Payload does not have the proper structure to consume');
        }
    } catch (err) {
        sendTeamsMessage(webHook, `Payload could not be processed because of error: ${err}`, 'Notification Group');
        console.error('An error occurred while processing the payload:', err);
        res.status(500).send('An error occurred while processing the payload');
    }
});

module.exports = router;


module.exports = router;
