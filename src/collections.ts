import * as fs from 'fs/promises';
import collections from '../collections.json' assert { type: "json" };


for (const collection of collections) {
    // Create an array to store the collection data
    const collectionData = [];

    // Process collections in batches of 40
    const batchSize = 40;
    const delay = 1000; // 1 second in milliseconds

    for (let i = 0; i < collections.length; i += batchSize) {
        const batch = collections.slice(i, i + batchSize);

        // Process batch concurrently
        const batchPromises = batch.map(async (item) => {
            const id = item.collection.split(":")[1];
            try {
                const url = `https://api.themoviedb.org/3/collection/${ id }?language=en-US`;
                const request = new Request(url, {
                    headers: {
                        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIzZDNmMmUwOTJjMWFmMDVlM2QyZDJhNDJjYmZlYjc0OCIsIm5iZiI6MTY5MTc5MjQ3MS44NTksInN1YiI6IjY0ZDZiNDU3ZDEwMGI2MDExYzgxMzBkMyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.jkjjEnDb8Sk4x4deduBooe81gOGIrUkIk44ocNuTfxg",
                    }
                });
                const response = await fetch(request);
                const data = await response.json();
                return data;
            } catch (error) {
                console.error(`Error fetching collection ${ id }:`, error);
                return null;
            }
        });

        // Wait for all requests in the batch to complete
        const results = await Promise.all(batchPromises);
        collectionData.push(...results.filter(data => data !== null));

        // If not the last batch, wait before the next batch
        if (i + batchSize < collections.length) {
            console.log(`Processed ${ i + batch.length } of ${ collections.length } collections. Waiting for rate limit...`);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }

    // Write the data to a file
    await fs.writeFile('collection_data.json', JSON.stringify(collectionData, null, 2));
    console.log(`Saved data for ${ collectionData.length } collections to collection_data.json`);
    process.exit(0)
}