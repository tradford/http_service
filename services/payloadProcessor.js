const config = require('../db/dbConfig');
const {insertShipment,
    updateDelinquentStatus,
    insertWaybill,
    insertDiversionWaybill,
    insertLoad,
    //insertSeals,
    insertShipper,
    insertConsignee,
    insertBillingItem,
    insertCharge,
    insertChargeAndDebitsCredits,
    insertAdditionalInvoiceInfo,
    insertRevenueRoute} = require('../db/index')
const sql = require('mssql');

const { sendTeamsMessage, logMessage } = require('../utils/msteam');

const taskQueue = [];
let isProcessing = false;

const webHook = 'webhook' // Add webhook



async function checkIfIdenticalOrZeroDue(payload, transaction, webHook) {
    const baseId = payload.id;
    console.log(`${baseId}`);

    // Query to check existing versions of the shipment
    const checkQuery = `
        SELECT colns
        FROM your_table_name_here 
        WHERE colns LIKE @val
        ORDER BY colns DESC`;

    const valPattern = baseId + '%'; // Pattern to match any version of the ID
    const checkResult = await transaction.request()
        .input('val', sql.NVarChar, valPattern)
        .query(checkQuery);
    console.log(checkResult.recordset.length);

    if (checkResult.recordset.length > 0) {
        let isIdentical = false;

        for (const record of checkResult.recordset) {
            // Convert SQL date strings to JavaScript Date objects for comparison
            const recordDate1 = new Date(record.colns).getTime();
            const payloadDate1 = new Date(payload.colns).getTime();
            const recordDate2 = new Date(record.colns).getTime();
            const payloadDate2 = new Date(payload.colns).getTime();

            if (record.colns === payload.colns && 
                record.colns === payload.colns && 
                recordDate1 === payloadDate1 && 
                recordDate2 === payloadDate2 && 
                record.colns === payload.colns &&
                record.colns === payload.colns) {
                isIdentical = true;
                break;
            }
        }

        if (isIdentical || payload.colns === 0) {
            console.log('Skipping due to identical payload or net amount due is zero');
            await sendTeamsMessage(webHook, 'INVOICE: ' + payload.id + ' skipped processing due to being identical or net amount due is zero.', 'Notification Group');
            return true; // Skip further processing
        }
    }

    return false; // Continue with processing
}

// Rest of your database connection code...

async function processPayload(payload) {
    let pool;
    let transaction;
    try {
        pool = await sql.connect(config);
        transaction = new sql.Transaction(pool);
        await transaction.begin();
        logMessage('path_to_log_file', 'Connected to SQL successfully!');

        const shouldSkipProcessing = await checkIfIdenticalOrZeroDue(payload, transaction, webHook);
        if (shouldSkipProcessing) {
            await transaction.commit(); // It's important to commit the transaction even if no changes were made
            return; // Exit the function early
        }

        // DIVERSION PAYLOAD
        const diversionItemExists = payload.billing_items && payload.billing_items.some(
            item => item.billing_item_type_code === 'DIVERSION');

        if (diversionItemExists) {
            logMessage('path_to_log_file', 'Attempting to process Diversion payload: ' + payload.id)
            const newId = await insertShipment(payload, transaction);
            await insertDiversionWaybill(payload, transaction, newId);
            await insertShipper(payload, transaction, newId);
            await insertConsignee(payload, transaction, newId);
            
            logMessage('path_to_log_file', 'Diversion Invoice: ' +  payload.id + ' was added to the database successfully');
            await sendTeamsMessage(webHook, 'Diversion Invoice: ' +  payload.id + ' was added to the database successfully', 'Recipient1');
            await sendTeamsMessage(webHook, 'Diversion Invoice: ' +  payload.id + ' was added to the database successfully', 'Recipient2');
            await sendTeamsMessage(webHook, 'Diversion Invoice: ' +  payload.id + ' was added to the database successfully', 'Recipient3');
            await sendTeamsMessage(webHook, 'Diversion Invoice: ' +  payload.id + ' was added to the database successfully', 'Recipient4');
        }

        //FREIGHT PAYLOAD
        if (payload.account_type_code === 'FREIGHT' && 
            !payload.billing_items.some(item => item.billing_item_type_code === 'FINANCE_CHARGE')) {
            logMessage('path_to_log_file', 'Attempting to process Freight payload: ' + payload.id)
            const newId = await insertShipment(payload, transaction);
            const newWaybillNumber = await insertWaybill(payload, transaction, newId);
            for (const waybill of payload.waybills) {
                if (waybill.loads && Array.isArray(waybill.loads)) { 
                    for (const load of waybill.loads) {
                        await insertLoad(load, waybill, waybill.id, transaction, newId, newWaybillNumber);
                    }
                }
            }
            
            logMessage('path_to_log_file', 'Freight Invoice: ' +  payload.id + ' was added to the database successfully');
            await sendTeamsMessage(webHook, 'Freight Invoice: ' +  payload.id + ' was added to the database successfully', 'Recipient1');
            await sendTeamsMessage(webHook, 'Freight Invoice: ' +  payload.id + ' was added to the database successfully', 'Recipient2');
            await sendTeamsMessage(webHook, 'Freight Invoice: ' +  payload.id + ' was added to the database successfully', 'Recipient3');
            await sendTeamsMessage(webHook, 'Freight Invoice: ' +  payload.id + ' was added to the database successfully', 'Recipient4');

        }
    
        // SWITCH PAYLOAD
        if (payload.route_code === 'INTRA_PLANT_SWITCH') {
            logMessage('path_to_log_file', 'Attempting to process Switch payload: ' + payload.id)
            const newId = await insertShipment(payload, transaction);
            await insertWaybill(payload, transaction, newId);
            await insertLoad(payload.waybills.loads, payload.waybills, payload.waybills.id, transaction, newId, newWaybillNumber);
            await insertRevenueRoute(payload, transaction);
            await insertShipper(payload, transaction, newId);
            await insertConsignee(payload, transaction, newId);
        }

        //CES PAYLOAD
        if (payload.account_type_code === 'DEMURRAGE') {
            logMessage('path_to_log_file', 'Attempting to process demurrage payload: ' + payload.id)
            const newId = await insertShipment(payload, transaction);
            await insertConsignee(payload, transaction, newId);
        }

        // Customer Exception PAYLOAD
        const customerExceptionExists = payload.billing_items && payload.billing_items.some(
            item => item.billing_item_type_code === 'CUSTOMER_EXCEPTION');
        if (customerExceptionExists) {
            logMessage('path_to_log_file', 'Attempting to process Customer Exception payload: ' + payload.id)
            const newId = await insertShipment(payload, transaction);
            logMessage('path_to_log_file', 'Customer Exception Invoice: ' +  payload.id + ' was added to the database successfully');
        }

        //Financial charge PAYLOAD
        if (payload.account_type_code === 'FREIGHT' && 
            payload.billing_items.some(item => item.billing_item_type_code === 'FINANCE_CHARGE')) {
            logMessage('path_to_log_file', 'Attempting to process Finance Charge: ' + payload.id)
            await insertShipment(payload, transaction);
            logMessage('path_to_log_file', 'Finance Charge Invoice: ' +  payload.id + ' was added to the database successfully');
        }

        await transaction.commit();
        console.log('All data inserted successfully');
    } catch (err) {
        if (transaction) {
            await transaction.rollback();
            logMessage('path_to_log_file', 'Transaction rolled back due to error:', err);
        }
        throw err; // Re-throw the error to be caught by the route handler
    } finally {
        if (pool) {
            await pool.close(); // Close the database connection
        }
    }
};



function addTaskToQueue(task) {
    taskQueue.push(task);
    processNextTask(); // Make sure this function exists and is defined properly
}


async function processNextTask() {
    if (isProcessing || taskQueue.length === 0) return;
    
    isProcessing = true;
    const task = taskQueue.shift(); // Get the first task from the queue
    await task(); // Execute the task
    
    isProcessing = false;
    processNextTask(); // Check for more tasks in the queue
}

function addPayloadToProcess(payload) {
    const task = () => processPayload(payload); // Wrap the processPayload call
    addTaskToQueue(task);
}



module.exports = {
    addPayloadToProcess,
};
