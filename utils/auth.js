const jwt = require('jsonwebtoken');
//const passport = require('passport');

// Define your authentication middleware and other auth-related functions
function authenticateToken(req, res, next) {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    if (!token) return res.sendStatus(401); // If there's no token, return 401 Unauthorized
    jwt.verify(token, 'your-jwt-secret', (err, user) => {
        if (err) return res.sendStatus(403); // If token is invalid, return 403 Forbidden
        req.user = user;
        next();
    });
}

module.exports = {
    authenticateToken,
    // ... other exported functions ...
};
