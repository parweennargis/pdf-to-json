require('dotenv').config();
const express = require('express');
const AWS = require('aws-sdk');
const pdf = require('pdf-parse');
const { MongoClient } = require('mongodb');
const cron = require('node-cron');

// Configure AWS S3
const s3 = new AWS.S3({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
});

const app = express();
const port = process.env.PORT || 3000;

// MongoDB Connection URI
const mongoUri = process.env.MONGODB_URI;
const client = new MongoClient(mongoUri, { useNewUrlParser: true, useUnifiedTopology: true });

// Function to fetch and process PDFs from S3
async function fetchAndProcessPdfs() {
    try {
        // Connect to MongoDB
        await client.connect();
        const database = client.db('pdfToJson');
        const collection = database.collection('pdfs');

        // Define your S3 bucket and parameters
        const bucketName = 'dubai-clinic';
        const params = {
            Bucket: bucketName,
        };

        // List objects in the bucket
        const data = await s3.listObjectsV2(params).promise();

        // Filter for PDF files based on last modified date
        const pdfFiles = data.Contents.filter(file => 
            file.Key.endsWith('.pdf') && 
            file.LastModified > new Date(Date.now() - 24 * 60 * 60 * 1000) // Last 24 hours
        );

        for (const file of pdfFiles) {
            // check if file exist with the date
            const fileExist = await collection.findOne({
                fileName: file.Key,
                lastModified: file.LastModified
            });
            if (!fileExist) {
                // Get the PDF file from S3
                const fileParams = {
                    Bucket: bucketName,
                    Key: file.Key,
                };
                const pdfData = await s3.getObject(fileParams).promise();

                // Parse the PDF file
                const pdfText = await pdf(pdfData.Body);
                
                // Prepare the JSON object
                const jsonData = {
                    fileName: file.Key,
                    content: pdfText.text,
                    lastModified: file.LastModified,
                };

                // Insert JSON data into MongoDB
                await collection.insertOne(jsonData);
                console.log(`Stored PDF data for ${file.Key}`);
            }
        }
    } catch (error) {
        console.error("Error processing PDFs:", error);
    } finally {
        await client.close();
    }
}

// Create an endpoint to trigger the PDF processing
app.get('/process-pdfs', async (req, res) => {
    console.log('Received request to process PDFs.');
    try {
        await fetchAndProcessPdfs();
        res.status(200).send('PDFs processed successfully.');
    } catch (error) {
        console.error("Error processing PDFs via endpoint:", error);
        res.status(500).send('Error processing PDFs.');
    }
});

// Start the server
app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});

// Schedule the task to run daily at midnight
cron.schedule('0 0 * * *', () => {
    console.log('Running scheduled task to fetch PDFs from S3 and store in MongoDB...');
    fetchAndProcessPdfs();
});

// Start the cron job immediately for testing
// fetchAndProcessPdfs();

 // Export the app for Vercel
 module.exports = app;