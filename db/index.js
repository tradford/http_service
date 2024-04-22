const sql = require('mssql');
const { sendTeamsMessage, logMessage } = require('../utils/msteam');
const { v4: uuidv4 } = require('uuid');
const schemaName = "schema_name"; // Add schema name

const webHook = 'Teams Webhook' // Add webhook

async function insertShipment(payload, transaction) {
    try {
        let billOfLadingDate = payload.bill_of_lading_date ? new Date(payload.bill_of_lading_date) : null;
    // Ensure billOfLadingDate is either a valid Date object or null
    if (billOfLadingDate && isNaN(billOfLadingDate.getTime())) {
        billOfLadingDate = null;
    }
        // Base ID without version suffix
        const baseId = payload.id;
        console.log(`${baseId}`);

        // Query to check existing versions of the shipment
        const checkQuery = `
            SELECT columns FROM ${schemaName}.Table_Name WHERE id LIKE @idPattern
            ORDER BY id DESC`; // Add columns and table
        
        const idPattern = baseId + '%'; // Pattern to match any version of the ID
        const checkResult = await transaction.request()
            .input('idPattern', sql.NVarChar, idPattern)
            .query(checkQuery);
        console.log(checkResult.recordset.length)
        let newId = baseId; // Default to baseId if no previous versions found

        
        if (checkResult.recordset.length > 0 && payload.net_amount_due == 0) {

            await sendTeamsMessage(webHook, 'INVOICE: ' +  payload.id + ' Is due soon but ETS balance is 0' , 'Employee');
            await sendTeamsMessage(webHook, 'INVOICE: ' +  payload.id + ' Is due soon but ETS balance is 0', 'Employee');
       
        }else if (checkResult.recordset.length > 0) {
            let isIdentical = false;

        for (const record of checkResult.recordset) {
            // Example comparison for financial_account_number, you can add more fields to compare
            if (record.financial_account_number === payload.financial_account_number && 
                record.account_type_code === payload.account_type_code && 
                record.statement_date === payload.statement_date && 
                record.due_date === payload.due_date && 
                record.scheduled_reason_code === payload.scheduled_reason_code ) {
                isIdentical = true;
                break;
            }
        }

        if (isIdentical) {
            await sendTeamsMessage(webHook, 'INVOICE: ' +  payload.id + ' Seems to be identical, please check backup files to confirm', 'Employee');
        } else {
            
        

            // Extract the highest version number and increment for the new ID
            const latestId = checkResult.recordset[0].id;
            const versionMatch = latestId.match(/v(\d+)$/);
            const versionNumber = versionMatch ? parseInt(versionMatch[1], 10) + 1 : 2; // Start with v2 if no version suffix found
            newId = `${baseId}v${versionNumber}`; // Append new version number

            const shipmentInsertQuery = `
                    INSERT INTO ${schemaName}.Shipments (
                        columns // Add columns
                    ) 
                    VALUES (
                        @id, // Add other values
                    );
                `;
                console.log('Inserting into Shipments table a new version...');
                await transaction.request()
                    .input('id', sql.NVarChar, newId)
                    // Add other inputs
        
                    .query(shipmentInsertQuery);

          
            console.log('Shipment data inserted.');
  
                    }
                    }else {
            // If the shipment doesn't exist, proceed with insertion
            

            try {
                
                const shipmentInsertQuery = `
                    INSERT INTO ${schemaName}.Shipments (
                        id, // Add other columns
                    ) 
                    VALUES (
                        @id, // add other values
                    );
                `;
                console.log('Inserting into Shipments table...');
                await transaction.request()
                    .input('id', sql.NVarChar, payload.id)
                   // add additional inputs
        
                    .query(shipmentInsertQuery);

          
            console.log('Shipment data inserted.');

            // Update delinquent status
            await updateDelinquentStatus(payload.id, payload.net_amount_due, transaction);
            //return payload.id;
             // If you need to use the shipment ID later
        
        
    } catch (error) {
        console.error('Error in insertShipment inside of ID validation:', error);
        throw error;
    }
    
    }
    return newId;
    } catch (error) {
        console.error('Error in insertShipment:', error);
    }

}    
async function updateDelinquentStatus(shipmentId, netAmountDue, transaction, newId) {
    console.log(`net amount due ${netAmountDue}`)
    if (netAmountDue != 0.00) {
    try {
        const updateDelinquentSQL = `
            UPDATE ${schemaName}.Table_name
            SET delinquent = CASE
                WHEN GETDATE() > due_date THEN 1  -- Today is after the due date
                ELSE 0  -- Today is on or before the due date
            END
            WHERE id = @newId;`;

        await transaction.request()
            .input('newId', sql.NVarChar, newId)
            .query(updateDelinquentSQL);
        console.log(`Delinquent status updated for shipment ID ${shipmentId}.`);
    } catch (error) {
        console.error('Error updating delinquent status:', error);
        throw error;
    }
}
}


async function insertWaybill(payload, transaction, newId) {
    let processedWaybillNumbers = []; 
    for (const waybill of payload.waybills) {
        let waybillDate = waybill.waybill_date ? new Date(waybill.waybill_date) : null;
        if (waybillDate && isNaN(waybillDate.getTime())) {
            waybillDate = null;
        }

        // Define the base waybill number without version suffix
        const baseWaybillNumber = waybill.waybill_number;
        const checkQuery = `SELECT columns FROM ${schemaName}.table WHERE waybill_number LIKE @waybillNumberPattern AND shipmentId = @shipmentId
        ORDER BY waybill_number DESC`;
        const waybillNumberPattern = baseWaybillNumber + '%';
        const checkResult = await transaction.request()
            .input('waybillNumberPattern', sql.NVarChar, waybillNumberPattern)
            // Add inputs
            .query(checkQuery);
        if (!waybill.id) {
            waybill.id = uuidv4(); // Assign a new UUID if id is not present
        }
        let finalWaybillNumber = baseWaybillNumber; 


        if (checkResult.recordset.length > 0) {

            const latestWaybillNumber = checkResult.recordset[0].waybill_number;
            const versionMatch = latestWaybillNumber.match(/v(\d+)$/);
            const versionNumber = versionMatch ? parseInt(versionMatch[1], 10) + 1 : 2; // Start with v2 if no version suffix found
            finalWaybillNumber = `${baseWaybillNumber}v${versionNumber}`; // Append new version number
           
            console.log(`Adding waybill version ${finalWaybillNumber} with ID: ${waybill.id}...`);
            
            // Waybill exists, perform an update
            console.log(`Adding waybill version ${finalWaybillNumber}...`);
            await transaction.request()
            .input('id', sql.NVarChar, waybill.id)
            // Add inputs
            .query(`
                INSERT INTO ${schemaName}.Table (
                    id, // add columns
                ) 
                VALUES (
                    @id, // add values
                );
            `);
        
        } else {
            // Waybill does not exist, insert new record
            console.log(`Inserting waybill for the first time ${baseWaybillNumber}...`);
            await transaction.request()
                .input('id', sql.NVarChar, waybill.id)
                // Add inputs
                .query(`
                    INSERT INTO ${schemaName}.Table (
                        id, // add columns
                    ) 
                    VALUES (
                        @id, // add values
                    );
                `);
        }
        processedWaybillNumbers.push(finalWaybillNumber);

   
    }
    return processedWaybillNumbers[0];
}




async function insertDiversionWaybill(payload, transaction, newId) {
    try {
        if (!payload.waybills || payload.waybills.length === 0) {
            throw new Error('No waybills found in the payload.');
        }
        let newWaybillNumbers = [];
        for (const waybill of payload.waybills) {
        if (waybill.waybill_number){
            console.log("in if ")
        let newWaybillNumber;
        for (const waybill of payload.waybills) {
            if (waybill && waybill.waybill_number && waybill.loads) {
                // Check if the Diversion Waybill already exists
                const baseWaybillNumber = waybill.waybill_number;
                console.log(baseWaybillNumber)
                const checkQuery = `
                    SELECT waybill_number 
                    FROM ${schemaName}.Table 
                    WHERE  column LIKE @value AND column LIKE @value
                     ORDER BY waybill_number DESC`;
                     const shipmentId = payload.id;
                    const waybillNumberPattern = baseWaybillNumber + '%';
                    const shipmentIdPattern = shipmentId + '%';
                    const checkResult = await transaction.request()
                        .input('waybillNumberPattern', sql.NVarChar, waybillNumberPattern)
                        .input('shipmentIdPattern', sql.NVarChar, shipmentIdPattern)
                        .query(checkQuery);
                
                let newWaybillNumber = baseWaybillNumber;
                if (checkResult.recordset.length > 0) {

                    const latestWaybillNumber = checkResult.recordset[0].waybill_number;
                    console.log(`Latest waybill number from DB: ${latestWaybillNumber}`);
                    const versionMatch = latestWaybillNumber.match(/v(\d+)$/);
                    const versionNumber = versionMatch ? parseInt(versionMatch[1], 10) + 1 : 2; // Start with v2 if no version suffix found
                    newWaybillNumber = `${baseWaybillNumber}v${versionNumber}`; // Append new version number
                    // Diversion Waybill exists, perform an update
                    
                    console.log(`Adding  Diversion waybill version ${newWaybillNumber}...`);
            await transaction.request()
            .input('shipmentId', sql.NVarChar, newId)
            // add inputs
            .query(`
            INSERT INTO ${schemaName}.table (
                shipmentId, // add columns
            ) 
            VALUES (
                @shipmentId, // add values
            );
        `);

                    
                    
                } else {
                    // Diversion Waybill does not exist, insert new record
                    console.log(`Inserting Diversion waybill for the first time ${waybill.waybill_number}...`);
                    await transaction.request()
            .input('shipmentId', sql.NVarChar, payload.id)
            // add inputs
            .query(`
            INSERT INTO ${schemaName}.table (
                shipmentId, // add columns
            ) 
            VALUES (
                @shipmentId, // add values
            );
        `);
                   
                }

                // Insert or update related loads here if necessary
                // for (const load of waybill) {
                //     await insertLoad(load, waybill.id, waybill.waybill_number, transaction, newId); // Ensure this function is defined and updated for loads
                // }

                console.log(`Diversion Waybill ${waybill.waybill_number} processed with its loads.`);
                
            } else {
                console.log(`Skipping waybill insertion due to missing data for waybill number: ${waybill.waybill_number}`);
            }
        }
        
        newWaybillNumbers.push(newWaybillNumber);
    }else{ console.log("no waybill number, skipping")
    }
};

return newWaybillNumbers.length > 0 ? newWaybillNumbers[0] : undefined;
    } catch (error) {
        console.error('Error processing diversion waybill data:', error);
        throw error;
    }
   
}



// const processLoads = async (loads, waybillId) => {
//     for (const load of loads) {
//         console.log(`Processing Load ID: ${load.equipment.id}`);
//         await insertLoad(load, waybillId); // Function to insert load details

//         // Check if seals exist for this load
//         if (load.seals && load.seals.length > 0) {
//             await processSeals(load.seals, load.equipment.id); // Function to process seals
//         } else {
//             console.log(`No seals to process for Load ID: ${load.equipment.id}`);
//         }
//     }
// }



async function insertLoad(load, waybills, waybillNumber, transaction, newId, newWaybillNumber) {
    try {
        //waybills.loads = load
        if (load && load.equipment) {
            // Process load details
            const actual_net_weight = load.equipment.weight?.actual_net ?? null;
            const capacity = load.equipment.weight?.capacity ?? null;
            const exterior_length = load.equipment.dimensions?.exterior?.length ?? null;
            const exterior_width = load.equipment.dimensions?.exterior?.width ?? null;
            const estimated_net = load.equipment.weight?.estimated_net ?? null;
            const cubic_capacity = load.equipment.dimensions?.volume?.cubic_capacity ?? null;

            // Check if the load already exists
           
            const checkQuery = `SELECT columns FROM ${schemaName}.table WHERE column = @value 
            ORDER BY waybillId DESC`;
            
            const checkResult = await transaction.request()
               // .input('waybillNumberPattern', sql.NVarChar, waybillNumberPattern)
                .input('waybillId', sql.NVarChar, waybills.id)
                //.input('shipmentId', sql.NVarChar, shipmentId)
                .query(checkQuery);

            
            if (checkResult.recordset.length > 0) {
                // Load exists, perform an update
                // const latestWaybillNumber = checkResult.recordset[0].waybill_number;
                // const versionMatch = latestWaybillNumber.match(/v(\d+)$/);
                // const versionNumber = versionMatch ? parseInt(versionMatch[1], 10) + 1 : 2; // Start with v2 if no version suffix found
                // newWaybillNumber = `${baseWaybillNumber}v${versionNumber}`; // Append new version number
                // Diversion Waybill exists, perform an update
                
                console.log(`Adding load version for waybill Id ${newWaybillNumber}...`);
                // const updateQuery = `
                //     UPDATE ${schemaName}.Loads
                //     SET 
                //         waybillId = @waybillId, aar_type = @aar_type, 
                //         actual_net_weight = @actual_net_weight, capacity = @capacity, 
                //         exterior_length = @exterior_length, exterior_width = @exterior_width, 
                //         estimated_net = @estimated_net, cubic_capacity = @cubic_capacity
                //     WHERE id = @id;
                // `;
                await transaction.request()
                    .input('id', sql.NVarChar, load.equipment.id)
                    // add inputs
                    .query(`
                    INSERT INTO ${schemaName}.table (
                        id, // add columns
                    ) 
                    VALUES (
                        @id, // add values
                    );
                `);
            } else {
                // Load does not exist, insert new record
                console.log(`Inserting load for the first time ${load.equipment.id}...`);
                const insertQuery = `
                    INSERT INTO ${schemaName}.table (
                        id, // add columns 
                    ) 
                    VALUES (
                        @id,// add values
                    );
                `;
                await transaction.request()
                    .input('id', sql.NVarChar, load.equipment.id)
                   // add inputs
                    .query(insertQuery);
            }

            console.log(`Load ${load.equipment.id} processed successfully.`);
        } else {
            console.log(`Load data is missing or incomplete for waybill ID: ${waybills.id}. Skipping load insertion.`);
        }
    } catch (error) {
        console.error(`Error processing load for waybill ID ${waybills.id}:`, error);
        throw error; // Rethrow the error to be handled by the caller
    }
}


// async function insertSeal(sealNumber, waybillId, transaction) {
//     // Function to insert seal details
//     try {
//         const insertSealQuery = `
//             INSERT INTO ${schemaName}.table (waybillId, seal)
//             VALUES (@waybillId, @seal);
//         `;

//         await transaction.request()
//             .input('waybillId', sql.NVarChar, waybillId)
//             .input('seal', sql.NVarChar, sealNumber)
//             .query(insertSealQuery);

//         console.log(`Seal ${sealNumber} for Load ID ${waybillId} inserted successfully.`);
//     } catch (error) {
//         console.error(`Error inserting seal for Load ID ${waybillId}:`, error);
//         throw error; // Rethrow the error to be handled by the caller
//     }
// }


// Function to insert or update shipper information based on a shipment ID
async function insertShipper(payload, transaction, newId) {
    try {
        // Validate shipper details in the payload
        const shipper = payload.shipper;
        if (!shipper || !shipper.location) {
            throw new Error('Shipper or shipper location information is missing from the payload.');
        }

        // SQL query to check if the shipper already exists for the given shipment
        const checkQuery = `
            SELECT colns FROM ${schemaName}.table 
            WHERE colns = @val;
        `;
        const checkResult = await transaction.request()
            .input('val', sql.NVarChar, payload.id)
            .query(checkQuery);

        if (checkResult.recordset.length > 0) {
            // Shipper exists, perform an update
            console.log(`Updating shipper for shipmentId ${payload.id}...`);
            const updateQuery = `
                UPDATE ${schemaName}.table
                SET colns = @val
                WHERE colns = @val;
            `;
            await transaction.request()
                .input('val', sql.NVarChar, newId)
                .input('val', sql.NVarChar, shipper.name)
                .query(updateQuery);

            console.log('Shipper data updated.');
        } else {
            // Shipper does not exist, insert new record
            console.log('Inserting new shipper record...');
            const shipperInsertQuery = `
                INSERT INTO ${schemaName}.table (
                    colns, colns
                ) VALUES (
                    @val, @val
                );
            `;
            await transaction.request()
                .input('val', sql.NVarChar, payload.id)
                .input('val', sql.NVarChar, shipper.name)
                .query(shipperInsertQuery);

            console.log('Shipper data inserted.');
        }
    } catch (error) {
        console.error('Error processing shipper data:', error);
        throw error; // Rethrow the error to be handled by the caller
    }
}


// Function to insert or update consignee information based on a shipment ID
async function insertConsignee(payload, transaction, newId) {
    try {
        if (!payload.consignee || !payload.consignee.location) {
            throw new Error('Consignee or consignee location information is missing from the payload.');
        }

        const consignee = payload.consignee; // Consignee details are part of the payload
        // Check if the consignee already exists
        const checkQuery = `
            SELECT colns FROM ${schemaName}.table 
            WHERE colns LIKE @val;
        `;
        const checkResult = await transaction.request()
            .input('val', sql.NVarChar, payload.id)
            .query(checkQuery);

        if (checkResult.recordset.length > 0) {
            // Consignee exists, perform an update
            console.log(`Updating consignee for shipmentId ${payload.id}...`);
            const updateQuery = `
            UPDATE ${schemaName}.table SET
                colns = @val, colns = @val, colns = @val,
                colns = @val, colns = @val, colns = @val,
                colns = @val, colns = @val
            WHERE colns = @val;
            `;
            await transaction.request()
                .input('val', sql.NVarChar, newId)
                .input('val', sql.NVarChar, consignee.name)
                .input('val', sql.NVarChar, consignee.location.id)
                .input('val', sql.NVarChar, consignee.location.address)
                .input('val', sql.NVarChar, consignee.location.city)
                .input('val', sql.NVarChar, consignee.location.state_abbreviation)
                .input('val', sql.NVarChar, consignee.location.postal_code)
                .input('val', sql.NVarChar, consignee.location.country_abbreviation)
                .query(updateQuery);
        } else {
            // Consignee does not exist, insert new record
            console.log('Inserting into Consignee table...');
            const consigneeInsertQuery = `
                INSERT INTO ${schemaName}.table (
                    colns, colns, colns, colns,
                    colns, colns, colns, colns
                ) VALUES (
                    @val, @val, @val, @val,
                    @val, @val, @val, @val
                );
            `;
            await transaction.request()
                .input('val', sql.NVarChar, payload.id)
                .input('val', sql.NVarChar, consignee.name)
                .input('val', sql.NVarChar, consignee.location.id)
                .input('val', sql.NVarChar, consignee.location.address)
                .input('val', sql.NVarChar, consignee.location.city)
                .input('val', sql.NVarChar, consignee.location.state_abbreviation)
                .input('val', sql.NVarChar, consignee.location.postal_code)
                .input('val', sql.NVarChar, consignee.location.country_abbreviation)
                .query(consigneeInsertQuery);
        }

        console.log(`Consignee data processed successfully for shipmentId ${payload.id}.`);
    } catch (error) {
        console.error('Error processing consignee data:', error);
        throw error; // Rethrow the error to be handled by the transaction's catch block
    }
}





// Function to insert or update billing items associated with a shipment
async function insertBillingItem(billingItem, shipmentId, transaction, newId) {
    try {
        if (!billingItem) {
            throw new Error('No billing item provided.');
        }

        // Check if the billing item already exists
        const checkQuery = `
            SELECT colns FROM ${schemaName}.table 
            WHERE colns LIKE @val AND colns = @val;
        `;
        const checkResult = await transaction.request()
            .input('val', sql.NVarChar, shipmentId)
            .input('val', sql.Int, billingItem.line_item_number)
            .query(checkQuery);

        let billingItemId;
        const commodity = billingItem.commodity || {};
        const stcc = commodity.stcc || null;
        const commodityDescription = commodity.description || null;

        if (checkResult.recordset.length > 0) {
            // Billing item exists, perform an update
            console.log(`Updating billing item for shipmentId ${shipmentId} and line item number ${billingItem.line_item_number}...`);
            const updateQuery = `
                UPDATE ${schemaName}.table SET
                    colns = @val, colns = @val, colns = @val,
                    colns = @val, colns = @val
                WHERE colns = @val;
                SELECT SCOPE_IDENTITY() AS BillingItemID;
            `;
            await transaction.request()
                .input('val', sql.NVarChar, newId)
                .input('val', sql.NVarChar, billingItem.billing_item_type_code || null)
                .input('val', sql.Int, billingItem.line_item_number)
                .input('val', sql.NVarChar, stcc)
                .input('val', sql.NVarChar, commodityDescription)
                .query(updateQuery);

            // Assuming the first record contains the ID
            billingItemId = checkResult.recordset[0].id;
            console.log(`Updated billing item ID: ${billingItemId}`);
        } else {
            // Billing item does not exist, insert new record
            console.log('Inserting into BillingItems table...');
            const billingItemInsertQuery = `
                INSERT INTO ${schemaName}.table (
                    colns, colns, colns, colns, colns
                ) VALUES (
                    @val, @val, @val, @val, @val
                );
                SELECT SCOPE_IDENTITY() AS BillingItemID;
            `;
            const billingItemResult = await transaction.request()
                .input('val', sql.NVarChar, shipmentId)
                .input('val', sql.NVarChar, billingItem.billing_item_type_code || null)
                .input('val', sql.Int, billingItem.line_item_number)
                .input('val', sql.NVarChar, stcc)
                .input('val', sql.NVarChar, commodityDescription)
                .query(billingItemInsertQuery);

            billingItemId = billingItemResult.recordset[0].BillingItemID;
            console.log(`Inserted billing item ID: ${billingItemId}`);
        }

        return billingItemId;
    } catch (error) {
        console.error('Error processing billing items:', error);
        throw error; // Rethrow the error to be handled by the transaction's catch block
    }
}



  


// Function to insert or update charge details associated with a billing item in a shipment
async function insertCharge(charge, billingItemId, shipmentId, transaction, newId) {
    try {
        if (!charge) {
            throw new Error('No charge object provided.');
        }

        // Check if the charge already exists
        const checkQuery = `
            SELECT colns FROM ${schemaName}.table
            WHERE colns = @val AND colns LIKE @val AND colns = @val;
        `;
        const checkResult = await transaction.request()
            .input('val', sql.NVarChar, charge.rate_type_code)
            .input('val', sql.NVarChar, shipmentId)
            .input('val', sql.Int, billingItemId)
            .query(checkQuery);

        if (checkResult.recordset.length > 0) {
            // Charge exists, perform an update
            console.log(`Updating charge for billing item ${billingItemId}...`);
            const updateQuery = `
                UPDATE ${schemaName}.table SET
                    colns = @val, colns = @val, colns = @val,
                    colns = @val, colns = @val, colns = @val,
                    colns = @val, colns = @val, colns = @val,
                    colns = @val
                WHERE colns = @val;
            `;
            await transaction.request()
                .input('val', sql.Float, charge.charge)
                .input('val', sql.Float, charge.rate)
                .input('val', sql.NVarChar, newId)
                .input('val', sql.NVarChar, charge.rate_type_code)
                .input('val', sql.Float, charge.rate_quantity)
                .input('val', sql.Float, charge.weight)
                .input('val', sql.Float, charge.prepaid_amount)
                .input('val', sql.NVarChar, charge.reference_qualifier_code)
                .input('val', sql.NVarChar, charge.stac)
                .input('val', sql.NVarChar, charge.issuing_carrier)
                .input('val', sql.NVarChar, charge.tariff_item_number)
                .query(updateQuery);
        } else {
            // Charge does not exist, insert new record
            console.log('Inserting new charge record...');
            const chargeInsertQuery = `
                INSERT INTO ${schemaName}.table (
                    colns, colns, colns, colns, colns, colns, 
                    colns, colns, colns, colns, colns
                ) VALUES (
                    @val, @val, @val, @val, @val, @val, 
                    @val, @val, @val, @val, @val
                );
            `;
            await transaction.request()
                .input('val', sql.Int, billingItemId)
                .input('val', sql.Float, charge.charge)
                .input('val', sql.Float, charge.rate)
                .input('val', sql.NVarChar, charge.rate_type_code)
                .input('val', sql.Float, charge.rate_quantity)
                .input('val', sql.Float, charge.weight)
                .input('val', sql.Float, charge.prepaid_amount)
                .input('val', sql.NVarChar, charge.reference_qualifier_code)
                .input('val', sql.NVarChar, charge.stac)
                .input('val', sql.NVarChar, charge.issuing_carrier)
                .input('val', sql.NVarChar, charge.tariff_item_number)
                .query(chargeInsertQuery);
        }

        console.log(`Charge processing for billing item ${billingItemId} completed.`);
    } catch (error) {
        console.error('Error processing charge:', error);
        throw error; // Rethrow the error to be handled by the transaction's catch block
    }
}




// Function to insert or update charges and their associated debits and credits
async function insertChargeAndDebitsCredits(charge, billingItemId, shipmentId, transaction) {
    try {
        if (!charge) {
            throw new Error('No charge object provided.');
        }

        // Insert into Charges table
        const chargeInsertQuery = `
            INSERT INTO ${schemaName}.table (colns, colns, colns, colns)
            VALUES (@val, @val, @val, @val);
            SELECT SCOPE_IDENTITY() AS ChargeID;
        `;
        const chargeResult = await transaction.request()
            .input('val', sql.NVarChar, shipmentId)
            .input('val', sql.Int, billingItemId)
            .input('val', sql.Float, charge.charge)
            .input('val', sql.Date, charge.date)
            .query(chargeInsertQuery);

        const chargeId = chargeResult.recordset[0].ChargeID;
        console.log(`Retrieved ChargeId: ${chargeId}`);

        try {
            if (charge.debits_credits) {
                const { debits, credits } = charge.debits_credits;

                // Process debits and credits
                if (Array.isArray(debits) && debits.length > 0) {
                    // Insert Debits
                    for (const entry of debits) {
                        const debitInsertQuery = `
                            INSERT INTO ${schemaName}.table (ChargeId, colns, colns, colns, colns)
                            VALUES (@ChargeId, @val, @val, @val, @val);
                        `;
                        await transaction.request()
                            .input('ChargeId', sql.Int, chargeId)
                            .input('val', sql.NVarChar, shipmentId)
                            .input('val', sql.NVarChar, entry.debit_code)
                            .input('val', sql.Float, entry.debits)
                            .input('val', sql.NVarChar, 'DEBIT')
                            .query(debitInsertQuery);
                    }
                }

                if (Array.isArray(credits) && credits.length > 0) {
                    // Insert Credits
                    for (const entry of credits) {
                        const creditInsertQuery = `
                            INSERT INTO ${schemaName}.table (ChargeId, colns, colns, colns, colns)
                            VALUES (@ChargeId, @val, @val, @val, @val);
                        `;
                        await transaction.request()
                            .input('ChargeId', sql.Int, chargeId)
                            .input('val', sql.NVarChar, shipmentId)
                            .input('val', sql.NVarChar, entry.credit_code)
                            .input('val', sql.Float, entry.credits)
                            .input('val', sql.NVarChar, 'CREDIT')
                            .query(creditInsertQuery);
                    }
                }
            } else {
                console.log('No debits_credits found in the charge object.');
            }
        } catch (error) {
            console.error('Error inserting debit/credit entries:', error);
            throw error;
        }
        
    } catch (error) {
        console.error('Error inserting charge and debits/credits:', error);
        throw error; // Rethrow the error to be handled by the transaction's catch block
    }
}



// Function to insert equipment storage charges and associated details
async function insertEquipmentStorageCharges(payload, shipmentId, transaction) {
    try {
        for (const charge of payload.equipment_storage_charges) {
            // Extract and insert equipment details
            const equipmentInsertQuery = `
                INSERT INTO ${schemaName}.table (colns, colns, colns, colns, colns, colns, colns)
                VALUES (@val, @val, @val, @val, @val, @val, @val);
            `;
            await transaction.request()
                .input('val', sql.NVarChar, charge.equipment.id)
                .input('val', sql.NVarChar, shipmentId)
                .input('val', sql.NVarChar, charge.equipment.owner_type_code)
                .input('val', sql.Int, charge.equipment.total_debits)
                .input('val', sql.NVarChar, charge.equipment.arrived_at_serving_area)
                .input('val', sql.NVarChar, charge.equipment.released_from_industry)
                .input('val', sql.NVarChar, charge.equipment.original_eta)
                .query(equipmentInsertQuery);

            // Insert waybill details
            const waybillInsertQuery = `
                INSERT INTO ${schemaName}.table (colns, colns, colns, colns)
                VALUES (@val, @val, @val, @val);
            `;
            await transaction.request()
                .input('val', sql.NVarChar, charge.waybill.waybill_number)
                .input('val', sql.Date, new Date(charge.waybill.waybill_date))
                .input('val', sql.NVarChar, charge.waybill.load_empty_code)
                .input('val', sql.NVarChar, charge.waybill.route.origin.location.id)
                .query(waybillInsertQuery);

            // Handle any additional information as needed
            // This may include other details related to the shipment or charge
            console.log(`Charge details for equipment ${charge.equipment.id} inserted successfully.`);
        }
    } catch (error) {
        console.error('Error inserting equipment storage charges:', error);
        throw error; // Rethrow the error to be handled by the caller
    }
}


// Function to insert additional invoice information associated with a shipment
async function insertAdditionalInvoiceInfo(additionalInfo, payload, transaction) {
    try {
        // Validate that additionalInfo object contains necessary fields
        if (additionalInfo && additionalInfo.requested_by && additionalInfo.diverted_at_station && additionalInfo.original_consignee && additionalInfo.original_destination) {
            const insertQuery = `
                INSERT INTO ${schemaName}.table (
                    colns, colns, colns, colns, colns, colns, colns, colns, colns, colns, colns, colns, colns, colns
                ) VALUES (
                    @val, @val, @val, @val, @val, @val, @val, @val, @val, @val, @val, @val, @val, @val
                );
            `;
            await transaction.request()
                .input('val', sql.NVarChar, payload.id)
                .input('val', sql.NVarChar, additionalInfo.requested_by.first)
                .input('val', sql.NVarChar, additionalInfo.requested_by.last)
                .input('val', sql.NVarChar, additionalInfo.requested_by.user_id)
                .input('val', sql.Date, additionalInfo.requested_date)
                .input('val', sql.NVarChar, additionalInfo.diverted_at_station.id)
                .input('val', sql.NVarChar, additionalInfo.diverted_at_station.name)
                .input('val', sql.NVarChar, additionalInfo.diverted_at_station.state_abbreviation)
                .input('val', sql.NVarChar, additionalInfo.diverted_at_station.country_abbreviation)
                .input('val', sql.NVarChar, additionalInfo.original_consignee.name)
                .input('val', sql.NVarChar, additionalInfo.original_destination.id)
                .input('val', sql.NVarChar, additionalInfo.original_destination.city)
                .input('val', sql.NVarChar, additionalInfo.original_destination.state_abbreviation)
                .input('val', sql.NVarChar, additionalInfo.original_destination.country_abbreviation)
                .query(insertQuery);

            console.log('Additional invoice information inserted successfully.');
        } else {
            console.log('No additional invoice information found or incomplete data.');
        }
    } catch (error) {
        console.error('Error inserting additional invoice information:', error);
        throw error; // Rethrow the error to be handled by the caller
    }
}


// Function to insert revenue route details into the database
async function insertRevenueRoute(payload, transaction) {
    try {
        // Check if the payload contains necessary fields for revenue routes
        if (payload && payload.revenue_routes && payload.revenue_routes.origin && payload.revenue_routes.destination) {
            const insertQuery = `
                INSERT INTO ${schemaName}.table (
                    colns, colns, colns, colns, colns, colns,
                    colns, colns, colns, colns, colns, colns
                ) VALUES (
                    @val, @val, @val, @val, @val, @val,
                    @val, @val, @val, @val, @val, @val
                );
            `;

            await transaction.request()
                .input('val', sql.NVarChar, payload.id)
                .input('val', sql.NVarChar, payload.revenue_routes.origin.id)
                .input('val', sql.Int, payload.revenue_routes.origin.splc)
                .input('val', sql.NVarChar, payload.revenue_routes.origin.city)
                .input('val', sql.NVarChar, payload.revenue_routes.origin.state_abbreviation)
                .input('val', sql.NVarChar, payload.revenue_routes.origin.country_abbreviation)
                .input('val', sql.NVarChar, payload.revenue_routes.destination.id)
                .input('val', sql.Int, payload.revenue_routes.destination.splc)
                .input('val', sql.NVarChar, payload.revenue_routes.destination.city)
                .input('val', sql.NVarChar, payload.revenue_routes.destination.state_abbreviation)
                .input('val', sql.NVarChar, payload.revenue_routes.destination.country_abbreviation)
                .input('val', sql.NVarChar, payload.revenue_routes.destination.carrier)
                .query(insertQuery);

            console.log('Revenue Routes inserted successfully.');
        } else {
            console.log('No additional revenue route information found or incomplete data.');
        }
    } catch (error) {
        console.error('Error inserting revenue route information:', error);
        throw error; // Rethrow the error to be handled by the caller
    }
}


// Export the functions
module.exports = {
    insertShipment,
    updateDelinquentStatus,
    insertWaybill,
    insertDiversionWaybill,
    insertLoad,
    // insertSeal,
    insertShipper,
    insertConsignee,
    //insertBillingItem,
    //insertCharge,
    //insertChargeAndDebitsCredits,
    //insertAdditionalInvoiceInfo,
    //insertRevenueRoute
};