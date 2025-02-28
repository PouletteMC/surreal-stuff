import { createReadStream } from "fs";
import readline from "readline";
import Surreal from "surrealdb";

interface Genre {
    id: number;
    name: string;
}

interface Collection {
    id: number;
    name?: string;
    poster_path?: string;
    backdrop_path?: string;
}

interface Movie {
    id: number;
    title?: string;
    budget?: number;
    overview?: string;
    poster_path?: string;
    release_date?: string;
    revenue?: number;
    runtime?: number;
    tagline?: string;
    vote_average?: number;
    original_language?: string;
    genres: Genre[];
    belongs_to_collection?: Collection;
}

/**
 * Reads a malformed JSON file with movie objects and pushes them directly to SurrealDB
 * Implements bulk operations and batching for maximum ingest performance
 */
async function sendMoviesToSurrealDB(
    inputPath: string,
    batchSize = 1000,
): Promise<void> {
    // Connect to SurrealDB
    const db = new Surreal();

    try {
        await db.connect('http://localhost:8000');
        await db.signin({
            username: 'root',
            password: 'root',
        });
        await db.use({ namespace: 'grooveguessr', database: 'development' });

        console.log("Connected to SurrealDB successfully");

        const readStream = createReadStream(inputPath, { encoding: "utf-8" });

        const rl = readline.createInterface({
            input: readStream,
            crlfDelay: Number.POSITIVE_INFINITY,
        });

        let objectBuffer = "";
        let openBraces = 0;
        let inQuotes = false;
        let escapeNext = false;
        let movieCount = 0;
        let batchCount = 0;
        let currentBatch: Movie[] = [];

        for await (const line of rl) {
            for (let i = 0; i < line.length; i++) {
                const char = line[i];
                objectBuffer += char;

                if (escapeNext) {
                    escapeNext = false;
                    continue;
                }

                if (char === "\\") {
                    escapeNext = true;
                    continue;
                }

                if (char === '"' && !escapeNext) {
                    inQuotes = !inQuotes;
                    continue;
                }

                if (!inQuotes) {
                    if (char === "{") {
                        openBraces++;
                    } else if (char === "}") {
                        openBraces--;

                        // If we've completed an object
                        if (openBraces === 0) {
                            try {
                                const movie = JSON.parse(objectBuffer) as Movie;
                                currentBatch.push(movie);
                                movieCount++;

                                // When we reach the batch size, send them to SurrealDB
                                if (currentBatch.length >= batchSize) {
                                    await sendBatchToSurrealDB(db, currentBatch);
                                    currentBatch = [];
                                    batchCount++;

                                    // Clear console and update progress
                                    console.clear();
                                    console.log(`Records created: ${ movieCount } (${ batchCount } batches)`);
                                }

                                // Reset buffer for next object
                                objectBuffer = "";
                            } catch (err) {
                                console.error("Error parsing object:", err);
                                objectBuffer = "";
                            }
                        }
                    }
                }
            }

            // Add newline if we're in the middle of an object
            if (objectBuffer && openBraces > 0) {
                objectBuffer += "\n";
            }
        }

        // Send any remaining movies in the final batch
        if (currentBatch.length > 0) {
            await sendBatchToSurrealDB(db, currentBatch);
            batchCount++;

            // Final update
            console.clear();
            console.log(`Records created: ${ movieCount } (${ batchCount } batches)`);
        }

        console.log(
            `Completed! Successfully inserted ${ movieCount } movies into SurrealDB in ${ batchCount } batches.`
        );
    } catch (error) {
        console.error("Error:", error);
    } finally {
        await db.close();
    }
}

/**
 * Sends a batch of movies directly to SurrealDB
 */
async function sendBatchToSurrealDB(
    db: Surreal,
    movies: Movie[],
): Promise<void> {
    // Begin a transaction
    await db.query('BEGIN TRANSACTION;');

    try {
        // Format the movies for DB insertion
        const formattedMovies = movies.map(formatMovieForDB);

        // Insert each movie
        for (const movie of formattedMovies) {
            await db.create('movie', movie);
        }

        // Commit the transaction
        await db.query('COMMIT TRANSACTION;');
    } catch (error) {
        // Rollback on error
        await db.query('CANCEL TRANSACTION;');
        throw error;
    }
}

/**
 * Properly escapes strings for SurrealDB
 */
function escapeStr(str: string | undefined): string {
    if (!str) return "";

    return str
        .replace(/\\/g, "\\\\") // Escape backslashes first
        .replace(/'/g, "\\'") // Escape single quotes
        .replace(/\n/g, "\\n") // Escape newlines
        .replace(/\r/g, "\\r") // Escape carriage returns
        .replace(/\t/g, "\\t") // Escape tabs
        .replace(/\f/g, "\\f"); // Escape form feed
}

/**
 * Formats a movie object for SurrealDB insertion
 */
function formatMovieForDB(movie: Movie): Record<string, any> {
    return {
        id: movie.id,
        title: movie.title || "",
        budget: movie.budget || 0,
        overview: movie.overview || "",
        poster: movie.poster_path || "/empty",
        release_date: movie.release_date ? new Date(movie.release_date) : new Date("0001-01-01"),
        revenue: movie.revenue || 0,
        runtime: movie.runtime || 0,
        tagline: movie.tagline || "",
        vote_average: movie.vote_average || 0,
        language: `language:${ movie.original_language || "en" }`,
        genre: movie.genres && movie.genres.length > 0
            ? movie.genres.map(genre => `genre:${ genre.id }`)
            : ["genre:0"],
        collection: movie.belongs_to_collection
            ? `collection:${ movie.belongs_to_collection.id }`
            : null
    };
}

// Entry point
if (import.meta.main) {
    const args = process.argv.slice(2);
    if (args.length < 1 || args.length > 2) {
        console.error(
            "Usage: bun run script.ts <input-json-file> [batch-size]"
        );
        process.exit(1);
    }

    const [inputFile] = args;
    const batchSize = args[1] ? Number.parseInt(args[1]) : 1000;

    console.log(
        `Processing ${ inputFile } and sending directly to SurrealDB...`
    );
    console.log(`Using batch size: ${ batchSize }`);

    sendMoviesToSurrealDB(inputFile, batchSize)
        .then(() => console.log("Processing complete!"))
        .catch((err) => {
            console.error("Error processing file:", err);
            process.exit(1);
        });
}

export { sendMoviesToSurrealDB };