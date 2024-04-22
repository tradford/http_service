async function sendTeamsMessage(channel, message, names) {
    let mentions = [];
    let mentionsText = names.map(name => {
        const nameParts = name.split(" ");
        const fname = nameParts[0];
        const lname = nameParts.slice(1).join(""); // Join all parts except the first to form the last name

        const email = fname[0].toLowerCase() + lname.toLowerCase() + "@envirotechservices.com";
        mentions.push({
            "type": "mention",
            "text": `<at>${fname} UPN</at>`,
            "mentioned": {
                "id": email,
                "name": name
            }
        });
        return `<at>${fname} UPN</at>`;
    }).join(", ");

    const payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "body": [
                        {
                            "type": "TextBlock",
                            "size": "Medium",
                            "weight": "Bolder",
                            "text": "Scheduled Task Automated Alert!",
                            "wrap": true
                        },
                        {
                            "type": "TextBlock",
                            "text": `Hi ${mentionsText}, ${message}`,
                            "wrap": true
                        }
                    ],
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.0",
                    "msteams": {
                        "entities": mentions
                    }
                }
            }
        ]
    };
    const headers = { "Content-Type": "application/json" };

    try {
        const response = await axios.post(channel, payload, { headers: headers });
        logMessage('C:/Users/esisvc/Projects/Monitoring/log/Teams_JS.txt', `MS Teams response: ${response.status} ${response.statusText}`);
        logMessage('C:/Users/esisvc/Projects/Monitoring/log/Teams_JS.txt', `${message}\n`);
    } catch (error) {
        console.error(`Error sending message to MS Teams: ${error}`);
    }
}
