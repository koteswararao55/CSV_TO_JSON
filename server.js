const express = require("express");
const fs = require("fs");
const csv = require("csv-parser");
require('dotenv').config({path: `${__dirname}/.env`})

const database = require("./db");
const Services = require("./service")(database);

const app = express();
const port = process.env.PORT || 3000;

// Route to handle CSV to JSON conversion and database insertion
app.get("/convert", (req, res) => {
	const csvFilePath = `${__dirname}/data.csv`;
	const formattedJson = [];

	// Creating CSV file into Stream, so that it can able to handle Bulk data
	fs.createReadStream(csvFilePath)
		.pipe(csv())
		.on("data", async (data) => {
			const jsonRow = await formatObject(data);
			const insertResult = await (await Services).insertData(jsonRow);
			formattedJson.push(insertResult);
		})
		.on("end", async () => {
			const result = await (await Services).getAgeDistribution();
			const percentages = result.rows[0];
			// Trim percentages to two decimal places
			for (const key in percentages) {
				percentages[key] = parseFloat(percentages[key]).toFixed(2);
			}
			console.log(percentages);
			res.json(percentages);
		});
});

async function formatObject(row) {
	let obj = {};

	Object.keys(row).forEach((key) => {
		const objectArray = key.split(".");

		category = objectArray[0].trim();
		if (objectArray.length === 1) {
			obj[category] = row[key].trim();
		} else {
			if (!obj[category]) {
				obj[category] = {};
			}
			obj = updateNestedObject(obj, objectArray, row[key]);
		}
	});
	return obj;
}

// Function to update a dynamic nested object with multiple keys
function updateNestedObject(obj, keysArray, value) {
	const newObj = { ...obj };
	let currentObj = newObj;

	// Traverse the keysArray and update the object
	for (let i = 0; i < keysArray.length; i++) {
		const key = keysArray[i].trim();
		if (i === keysArray.length - 1) {
			currentObj[key] = value.trim();
		} else {
			// If the key doesn't exist or isn't an object, create an empty object
			currentObj[key] = currentObj[key] || {};
			currentObj = currentObj[key];
		}
	}

	return newObj;
}

// Server starts listening
app.listen(port, () => {
	console.log(`Server is running on http://localhost:${port}`);
	console.log(`This is the URL for CSV to JSON conversion: http://localhost:${port}/convert`);
});
