const bcrypt = require('bcrypt');
const sql = require('mssql');
const dbConfig = require('../db/dbConfig');

const registerUser = async (username, plainTextPassword) => {
    try {
        const hash = await bcrypt.hash(plainTextPassword, 10);
        await sql.connect(databaseConfiguration);
        const result = await sql.query`INSERT INTO your_table (username_column, password_column) VALUES (${username}, ${hash})`;
        console.log('User created successfully');
    } catch (err) {
        console.error('Error:', err.message);
    }
};

const loginUser = async (inputUsername, inputPassword) => {
    try {
        await sql.connect(databaseConfiguration);
        const result = await sql.query`SELECT password_column FROM your_table WHERE username_column = ${inputUsername}`;
        
        if (result.recordset.length > 0) {
            const hash = result.recordset[0].password_column;
            const isMatch = await bcrypt.compare(inputPassword, hash);
            if (isMatch) {
                console.log('Login successful');
            } else {
                console.log('Login failed');
            }
        } else {
            console.log('User not found');
        }
    } catch (err) {
        console.error('Error:', err.message);
    }
};

// registerUser(, );
// loginUser(, );